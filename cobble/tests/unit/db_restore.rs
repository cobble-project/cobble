use super::*;

fn ids(refs: Vec<RestoreFileRef>) -> Vec<u64> {
    refs.into_iter().map(|item| item.file_id).collect()
}

#[test]
fn restore_ref_order_priority_policy() {
    let mut refs = vec![
        RestoreFileRef {
            file_id: 3,
            path: "a".to_string(),
            origin: ReplicaOrigin::Owned,
            size_bytes: 100,
            priority: 10,
            placement: PrimaryDataPlacement::Standard,
        },
        RestoreFileRef {
            file_id: 1,
            path: "b".to_string(),
            origin: ReplicaOrigin::Owned,
            size_bytes: 200,
            priority: 2,
            placement: PrimaryDataPlacement::Standard,
        },
        RestoreFileRef {
            file_id: 2,
            path: "c".to_string(),
            origin: ReplicaOrigin::Owned,
            size_bytes: 50,
            priority: 2,
            placement: PrimaryDataPlacement::Standard,
        },
    ];
    refs.sort_by(|left, right| {
        compare_primary_offload_file_refs(
            PrimaryVolumeOffloadPolicyKind::Priority,
            &PrimaryOffloadFileRef {
                file_id: left.file_id,
                size_bytes: left.size_bytes,
                priority: left.priority,
            },
            &PrimaryOffloadFileRef {
                file_id: right.file_id,
                size_bytes: right.size_bytes,
                priority: right.priority,
            },
        )
    });
    assert_eq!(ids(refs), vec![1, 2, 3]);
    assert_eq!(
        restore_source_origin(&ReplicaOrigin::Owned, Some("snapshot:source:7")),
        ReplicaOrigin::ExternalPersistent {
            source_id: "snapshot:source:7".to_string(),
        }
    );
}

#[test]
fn restore_ref_order_largest_file_policy() {
    let mut refs = vec![
        RestoreFileRef {
            file_id: 3,
            path: "a".to_string(),
            origin: ReplicaOrigin::Owned,
            size_bytes: 100,
            priority: 10,
            placement: PrimaryDataPlacement::Standard,
        },
        RestoreFileRef {
            file_id: 1,
            path: "b".to_string(),
            origin: ReplicaOrigin::Owned,
            size_bytes: 200,
            priority: 2,
            placement: PrimaryDataPlacement::Standard,
        },
        RestoreFileRef {
            file_id: 2,
            path: "c".to_string(),
            origin: ReplicaOrigin::Owned,
            size_bytes: 200,
            priority: 8,
            placement: PrimaryDataPlacement::Standard,
        },
    ];
    refs.sort_by(|left, right| {
        compare_primary_offload_file_refs(
            PrimaryVolumeOffloadPolicyKind::LargestFile,
            &PrimaryOffloadFileRef {
                file_id: left.file_id,
                size_bytes: left.size_bytes,
                priority: left.priority,
            },
            &PrimaryOffloadFileRef {
                file_id: right.file_id,
                size_bytes: right.size_bytes,
                priority: right.priority,
            },
        )
    });
    assert_eq!(ids(refs), vec![1, 2, 3]);
}
