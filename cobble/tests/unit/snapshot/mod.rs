use super::{DbSnapshot, SnapshotLifecycleState};

#[test]
fn cancel_wins_only_before_publication_starts() {
    let snapshot = DbSnapshot::new(7, "manifest", None);

    assert!(snapshot.try_cancel());
    assert_eq!(
        snapshot.lifecycle_state(),
        SnapshotLifecycleState::Cancelled
    );
    assert!(!snapshot.try_cancel());
    assert_eq!(
        snapshot.try_begin_publication(),
        Err(SnapshotLifecycleState::Cancelled)
    );
}

#[test]
fn publication_start_blocks_later_cancellation() {
    let snapshot = DbSnapshot::new(8, "manifest", None);

    assert_eq!(snapshot.try_begin_publication(), Ok(()));
    assert_eq!(
        snapshot.lifecycle_state(),
        SnapshotLifecycleState::CommitStarted
    );
    assert!(!snapshot.try_cancel());

    snapshot.mark_published();
    assert_eq!(
        snapshot.lifecycle_state(),
        SnapshotLifecycleState::Published
    );
    assert!(!snapshot.try_cancel());
}

#[test]
fn expire_request_is_folded_into_lifecycle_state() {
    let snapshot = DbSnapshot::new(9, "manifest", None);

    assert_eq!(snapshot.try_begin_publication(), Ok(()));
    assert!(snapshot.request_expire_after_publication_start());
    assert_eq!(
        snapshot.lifecycle_state(),
        SnapshotLifecycleState::CommitStartedExpireRequested
    );
    assert!(snapshot.mark_published());
    assert_eq!(
        snapshot.lifecycle_state(),
        SnapshotLifecycleState::Published
    );
}
