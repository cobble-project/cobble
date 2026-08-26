use cobble::{
    CoordinatorConfig, DbCoordinator, ShardSnapshotInput, VolumeDescriptor, VolumeUsageKind,
};
use cobble_table::snapshot::TableSnapshotCommitter;
use std::collections::BTreeMap;
use std::ops::RangeInclusive;
use std::sync::Arc;

#[test]
fn table_snapshot_committer_collects_and_commits_interleaved_checkpoints() {
    let root = tempfile::tempdir().unwrap();
    let coordinator = Arc::new(
        DbCoordinator::open(CoordinatorConfig {
            volumes: vec![VolumeDescriptor::new(
                format!("file://{}", root.path().display()),
                vec![VolumeUsageKind::Meta],
            )],
            snapshot_retention: None,
        })
        .unwrap(),
    );
    assert!(TableSnapshotCommitter::new(Arc::clone(&coordinator), 0, 2).is_err());
    assert!(TableSnapshotCommitter::new(Arc::clone(&coordinator), 4, 0).is_err());
    let committer = TableSnapshotCommitter::new(Arc::clone(&coordinator), 4, 2).unwrap();
    let input_a = shard_input("db-a", vec![0..=1], 1);
    let input_b = shard_input("db-b", vec![2..=3], 2);

    assert!(committer.submit(10, input_a.clone()).unwrap().is_none());
    assert!(committer.submit(11, input_a.clone()).unwrap().is_none());
    assert!(committer.submit(11, input_a.clone()).unwrap().is_none());
    let mut conflicting = input_a.clone();
    conflicting.snapshot_id = 99;
    assert!(committer.submit(11, conflicting).is_err());
    let snapshot_11 = committer.submit(11, input_b.clone()).unwrap().unwrap();
    assert_eq!(snapshot_11.id, 0);
    assert_ne!(snapshot_11.id, 11);
    assert_eq!(snapshot_11.shard_snapshots[0].db_id, "db-a");
    assert!(committer.submit(10, input_b.clone()).unwrap().is_none());

    // A complete batch bypasses the incremental pending window even when two newer incomplete
    // commits already occupy it.
    assert!(committer.submit(14, input_a.clone()).unwrap().is_none());
    assert!(committer.submit(15, input_a.clone()).unwrap().is_none());
    assert_eq!(
        committer
            .commit_batch(12, vec![input_b.clone(), input_a.clone()])
            .unwrap()
            .unwrap()
            .id,
        1
    );
    assert!(
        committer
            .commit_batch(12, vec![input_a.clone(), input_b.clone()])
            .unwrap()
            .is_none()
    );

    let mut reversed_a = input_a.clone();
    reversed_a.ranges.reverse();
    assert!(committer.submit(20, reversed_a).unwrap().is_none());
    assert!(committer.submit(21, input_a.clone()).unwrap().is_none());
    assert_eq!(
        committer.submit(20, input_b.clone()).unwrap().unwrap().id,
        2
    );
    assert_eq!(
        committer.submit(21, input_b.clone()).unwrap().unwrap().id,
        3
    );

    // The two-entry pending window evicts commit 40 when commit 42 arrives. Its late shard cannot
    // displace either retained newer commit.
    for commit_id in 40..=42 {
        assert!(
            committer
                .submit(commit_id, input_a.clone())
                .unwrap()
                .is_none()
        );
    }
    assert!(committer.submit(40, input_b.clone()).unwrap().is_none());
    assert_eq!(
        committer.submit(41, input_b.clone()).unwrap().unwrap().id,
        4
    );
    assert_eq!(
        committer.submit(42, input_b.clone()).unwrap().unwrap().id,
        5
    );

    let mut overlap = input_a.clone();
    overlap.ranges = vec![0..=2];
    assert!(committer.submit(50, overlap).unwrap().is_none());
    assert!(committer.submit(50, input_b.clone()).is_err());
    let mut out_of_bounds = input_a.clone();
    out_of_bounds.ranges = vec![0..=4];
    assert!(committer.submit(51, out_of_bounds).is_err());
    let mut gap = input_a.clone();
    gap.ranges = vec![0..=0];
    assert!(committer.submit(52, gap).unwrap().is_none());
    assert!(committer.submit(52, input_b.clone()).unwrap().is_none());

    let snapshot_60 = committer
        .commit_batch(60, vec![input_b.clone(), input_a.clone(), input_a.clone()])
        .unwrap()
        .unwrap();
    assert_eq!(snapshot_60.id, 6);
    assert_ne!(snapshot_60.id, 60);
    assert!(
        committer
            .commit_batch(60, vec![input_a.clone(), input_b.clone()])
            .unwrap()
            .is_none()
    );
    let mut changed_a = input_a.clone();
    changed_a.snapshot_id = 99;
    assert!(
        committer
            .commit_batch(61, vec![input_a.clone(), changed_a, input_b.clone()])
            .is_err()
    );
    assert!(committer.commit_batch(62, vec![input_a.clone()]).is_err());
    assert!(committer.submit(62, input_b.clone()).unwrap().is_none());
    assert_eq!(
        committer.submit(62, input_a.clone()).unwrap().unwrap().id,
        7
    );
    let mut batch_overlap = input_a.clone();
    batch_overlap.ranges = vec![0..=2];
    assert!(
        committer
            .commit_batch(63, vec![batch_overlap, input_b.clone()])
            .is_err()
    );
    assert!(
        committer
            .commit_batch(61, vec![input_a.clone(), input_b.clone()])
            .unwrap()
            .is_none()
    );

    // Commit ordering starts fresh in a replacement committer; the coordinator's snapshot IDs do
    // not reset or inherit the process-local commit ID.
    let new_session = TableSnapshotCommitter::new(Arc::clone(&coordinator), 4, 2).unwrap();
    let fresh_session_snapshot = new_session
        .commit_batch(1, vec![input_a.clone(), input_b.clone()])
        .unwrap()
        .unwrap();
    assert_eq!(fresh_session_snapshot.id, 8);
    assert_ne!(fresh_session_snapshot.id, 1);
    assert_eq!(
        coordinator
            .take_global_snapshot(4, vec![input_a, input_b])
            .unwrap()
            .id,
        9
    );
}

fn shard_input(
    db_id: &str,
    ranges: Vec<RangeInclusive<u16>>,
    snapshot_id: u64,
) -> ShardSnapshotInput {
    ShardSnapshotInput {
        ranges,
        column_family_ids: BTreeMap::from([("default".to_string(), 0)]),
        db_id: db_id.to_string(),
        snapshot_id,
        manifest_path: format!("file:///snapshots/{db_id}/{snapshot_id}"),
        timestamp_seconds: snapshot_id as u32,
        data_size_bytes: 10,
        incremental_data_size_bytes: 1,
    }
}
