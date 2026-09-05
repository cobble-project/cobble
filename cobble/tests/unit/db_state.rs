use super::*;

fn empty_version() -> Arc<LSMTreeVersion> {
    Arc::new(LSMTreeVersion { levels: Vec::new() })
}

#[test]
fn schema_minima_follow_current_files_and_column_families() {
    use crate::data_file::{DataFile, DataFileType};
    use crate::lsm::Level;
    use crate::schema::SchemaManager;

    let schemas = Arc::new(SchemaManager::new(1));
    let mut builder = schemas.builder();
    builder
        .add_column(0, None, None, Some("other".to_string()))
        .unwrap();
    builder.commit();
    let mut builder = schemas.builder();
    builder.add_column(1, None, None, None).unwrap();
    builder.commit();
    let target = schemas.builder().commit();
    let file_version = |id, schema_id| {
        Arc::new(LSMTreeVersion {
            levels: vec![Level {
                ordinal: 0,
                tiered: true,
                files: vec![Arc::new(DataFile::new_untracked(
                    DataFileType::SSTable,
                    b"a".to_vec(),
                    b"z".to_vec(),
                    id,
                    schema_id,
                    1,
                    0..=0,
                    0..=0,
                ))],
            }],
        })
    };
    let scopes = vec![
        LSMTreeScope::new(0..=0, 0),
        LSMTreeScope::new(1..=1, 0),
        LSMTreeScope::new(0..=1, 1),
    ];
    let original = MultiLSMTreeVersion::from_scopes_with_tree_versions(
        2,
        &scopes,
        vec![file_version(1, 0), file_version(2, 3), file_version(3, 1)],
    )
    .unwrap();
    let handle = DbStateHandle::new();
    let publish = |multi_lsm_version| {
        handle.store(DbState {
            seq_id: handle.allocate_seq_id(),
            topology_epoch: 0,
            bucket_ranges: vec![0..=1],
            multi_lsm_version,
            vlog_version: VlogVersion::new(),
            active: None,
            active_schema: None,
            min_source_schema_by_cf: Vec::new(),
            immutables: VecDeque::new(),
            truncation_cursors: new_truncation_cursors(),
            suggested_base_snapshot_id: None,
        })
    };
    publish(original.clone());
    let held = handle.load();
    assert_eq!(held.min_source_schema_id(0), Some(0));
    assert_eq!(held.min_source_schema_id(1), Some(1));
    assert!(held.scan_requires_schema_aware(&target, 0));
    assert!(!held.scan_requires_schema_aware(&target, 1));

    // Removing the oldest file must raise the minimum even while an old
    // snapshot still owns that file. An empty family contributes no minimum.
    publish(
        original
            .with_lsm_version_at(0, empty_version())
            .with_lsm_version_at(2, empty_version()),
    );
    let compacted = handle.load();
    assert_eq!(compacted.min_source_schema_id(0), Some(3));
    assert_eq!(compacted.min_source_schema_id(1), None);
    assert!(!compacted.scan_requires_schema_aware(&target, 0));
    assert_eq!(held.min_source_schema_id(0), Some(0));

    // Restore/layout replacement derives the metadata again, including a
    // decrease when older files become live in the current state once more.
    publish(original);
    assert_eq!(handle.load().min_source_schema_id(0), Some(0));
    assert!(handle.load().scan_requires_schema_aware(&target, 0));
}

fn cursor_map_ptr(store: &TruncationCursorStore) -> usize {
    let cursors = store.inner.current.read().unwrap();
    Arc::as_ptr(&*cursors) as usize
}

#[test]
fn test_truncation_cursor_store_updates_in_place_without_snapshot_reader() {
    let store = new_truncation_cursors();
    let id = TruncationCursorId::new(7, 3);
    let before = cursor_map_ptr(&store);

    store.advance(id.bucket, id.column_family_id, b"k1");

    let after = cursor_map_ptr(&store);
    assert_eq!(before, after);
    assert_eq!(
        store.get(id.bucket, id.column_family_id),
        Some(b"k1".to_vec())
    );
}

#[test]
fn test_truncation_cursor_store_copies_only_when_snapshot_holds_map() {
    let store = new_truncation_cursors();
    let id = TruncationCursorId::new(7, 3);
    store.advance(id.bucket, id.column_family_id, b"k1");
    let before = cursor_map_ptr(&store);

    let captured = store.capture();
    store.advance(id.bucket, id.column_family_id, b"k2");

    let after = cursor_map_ptr(&store);
    assert_ne!(before, after);
    assert_eq!(
        captured.to_map().get(&id).map(std::vec::Vec::as_slice),
        Some(b"k1".as_slice())
    );
    assert_eq!(
        store.get(id.bucket, id.column_family_id),
        Some(b"k2".to_vec())
    );
}

#[test]
fn test_multi_lsm_routes_by_bucket_and_column_family() {
    let version0 = empty_version();
    let version1 = empty_version();
    let version2 = empty_version();
    let scopes = vec![
        LSMTreeScope::new(0u16..=1u16, DEFAULT_COLUMN_FAMILY_ID),
        LSMTreeScope::new(2u16..=3u16, DEFAULT_COLUMN_FAMILY_ID),
        LSMTreeScope::new(0u16..=3u16, 1),
    ];
    let multi = MultiLSMTreeVersion::from_scopes_with_tree_versions(
        4,
        &scopes,
        vec![version0, version1, version2],
    )
    .expect("build multi lsm with CF scopes");

    assert_eq!(
        multi.tree_index_for_bucket_and_column_family(0, DEFAULT_COLUMN_FAMILY_ID),
        Some(0)
    );
    assert_eq!(
        multi.tree_index_for_bucket_and_column_family(3, DEFAULT_COLUMN_FAMILY_ID),
        Some(1)
    );
    assert_eq!(multi.tree_index_for_bucket_and_column_family(2, 1), Some(2));
    assert_eq!(multi.tree_index_for_bucket_and_column_family(2, 2), None);
    assert_eq!(
        multi.tree_index_for_bucket_and_column_family(0, DEFAULT_COLUMN_FAMILY_ID),
        Some(0)
    );
    assert_eq!(
        multi.tree_index_for_exact_scope(&LSMTreeScope::new(0u16..=3u16, 1)),
        Some(2)
    );
    assert_eq!(multi.tree_index_for_exact_range(&(0u16..=3u16)), None);
    assert_eq!(
        multi.bucket_range_of_tree(2),
        Some(0u16..=3u16),
        "scope range should be preserved"
    );
}

#[test]
fn test_multi_lsm_rejects_overlap_in_same_column_family() {
    let result = MultiLSMTreeVersion::from_scopes_with_tree_versions(
        4,
        &[
            LSMTreeScope::new(0u16..=2u16, DEFAULT_COLUMN_FAMILY_ID),
            LSMTreeScope::new(2u16..=3u16, DEFAULT_COLUMN_FAMILY_ID),
        ],
        vec![empty_version(), empty_version()],
    );
    assert!(matches!(
        result,
        Err(Error::ConfigError(msg)) if msg.contains("Overlapping bucket range")
    ));
}

#[test]
fn test_store_advances_sequence_allocator_past_restored_state() {
    let handle = DbStateHandle::new();
    let current = handle.load();
    handle.store(DbState {
        seq_id: 41,
        topology_epoch: current.topology_epoch,
        bucket_ranges: current.bucket_ranges.clone(),
        multi_lsm_version: current.multi_lsm_version.clone(),
        vlog_version: current.vlog_version.clone(),
        active: current.active.clone(),
        active_schema: current.active_schema.clone(),
        min_source_schema_by_cf: Vec::new(),
        immutables: current.immutables.clone(),
        truncation_cursors: current.truncation_cursors.clone(),
        suggested_base_snapshot_id: current.suggested_base_snapshot_id,
    });

    assert_eq!(handle.load().seq_id, 41);
    assert_eq!(handle.allocate_seq_id(), 42);

    // Storing a lower sequence later must not move the allocator backwards.
    let current = handle.load();
    handle.store(DbState {
        seq_id: 7,
        topology_epoch: current.topology_epoch,
        bucket_ranges: current.bucket_ranges.clone(),
        multi_lsm_version: current.multi_lsm_version.clone(),
        vlog_version: current.vlog_version.clone(),
        active: current.active.clone(),
        active_schema: current.active_schema.clone(),
        min_source_schema_by_cf: Vec::new(),
        immutables: current.immutables.clone(),
        truncation_cursors: current.truncation_cursors.clone(),
        suggested_base_snapshot_id: current.suggested_base_snapshot_id,
    });
    assert_eq!(handle.allocate_seq_id(), 43);

    handle.advance_next_seq_id(100);
    assert_eq!(handle.allocate_seq_id(), 100);
    handle.advance_next_seq_id(50);
    assert_eq!(handle.allocate_seq_id(), 101);
}

impl MultiLSMTreeVersion {
    pub(crate) fn from_parts(
        total_buckets: u32,
        bucket_to_tree_idx: Vec<u32>,
        tree_versions: Vec<Arc<LSMTreeVersion>>,
    ) -> Self {
        let mut entries = Vec::with_capacity(tree_versions.len());
        for (tree_idx, lsm_version) in tree_versions.into_iter().enumerate() {
            let mut start: Option<u16> = None;
            let mut end: Option<u16> = None;
            for (bucket, mapped) in bucket_to_tree_idx.iter().enumerate() {
                if *mapped != tree_idx as u32 {
                    continue;
                }
                let bucket = bucket as u16;
                if start.is_none() {
                    start = Some(bucket);
                }
                end = Some(bucket);
            }
            entries.push(TreeVersionEntry {
                scope: LSMTreeScope::new(
                    match (start, end) {
                        (Some(start), Some(end)) => start..=end,
                        _ => 0u16..=0u16,
                    },
                    DEFAULT_COLUMN_FAMILY_ID,
                ),
                lsm_version,
            });
        }
        Self {
            total_buckets,
            bucket_to_tree_idx_by_cf: vec![bucket_to_tree_idx],
            tree_versions: entries,
        }
    }
}
