use super::*;
use std::sync::mpsc;
use std::time::Duration;

impl DbLifecycle {
    fn active_access_count(&self) -> usize {
        self.active_accesses.load(Ordering::Acquire)
    }
}

#[test]
fn lifecycle_transitions_preserve_errors_and_reject_new_accesses_when_closing() {
    {
        let lifecycle = DbLifecycle::new_initializing();
        assert_eq!(lifecycle.state(), DbLifecycleState::Initializing);
        lifecycle.mark_open().unwrap();
        assert_eq!(lifecycle.state(), DbLifecycleState::Open);
        assert_eq!(
            lifecycle.begin_close().unwrap(),
            CloseTransition::Transitioned
        );
        assert_eq!(lifecycle.state(), DbLifecycleState::Closing);
        lifecycle.mark_closed();
        assert_eq!(lifecycle.state(), DbLifecycleState::Closed);
        assert_eq!(
            lifecycle.begin_close().unwrap(),
            CloseTransition::AlreadyClosingOrClosed
        );
    }

    {
        let lifecycle = DbLifecycle::new_initializing();
        let original = Error::IoError("boom".to_string());
        lifecycle.mark_error(original.clone());
        assert_eq!(lifecycle.state(), DbLifecycleState::Error);
        let err = lifecycle.ensure_open().unwrap_err();
        assert_eq!(err.to_string(), original.to_string());
    }

    {
        let lifecycle = DbLifecycle::new_open();
        assert_eq!(
            lifecycle.begin_close().unwrap(),
            CloseTransition::Transitioned
        );
        let err = lifecycle
            .begin_access()
            .err()
            .expect("begin_access should fail once close starts");
        assert!(err.to_string().contains("db is closing"));
    }
}

#[test]
fn close_waits_for_inflight_accesses() {
    let lifecycle = Arc::new(DbLifecycle::new_open());
    let access = lifecycle.begin_access().unwrap();
    assert_eq!(lifecycle.active_access_count(), 1);

    let lifecycle_for_thread = Arc::clone(&lifecycle);
    let (started_tx, started_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let handle = std::thread::spawn(move || {
        lifecycle_for_thread.begin_close().unwrap();
        started_tx.send(()).unwrap();
        lifecycle_for_thread.wait_for_accesses_to_drain();
        lifecycle_for_thread.mark_closed();
        done_tx.send(()).unwrap();
    });

    started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(done_rx.recv_timeout(Duration::from_millis(100)).is_err());

    drop(access);

    done_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    handle.join().unwrap();
    assert_eq!(lifecycle.state(), DbLifecycleState::Closed);
    assert_eq!(lifecycle.active_access_count(), 0);
}

#[test]
fn exclusive_access_drains_rejects_reopens_and_blocks_close() {
    let lifecycle = Arc::new(DbLifecycle::new_open());
    let normal = lifecycle.begin_access().unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let (exclusive_tx, exclusive_rx) = mpsc::channel();
    let lifecycle_for_exclusive = Arc::clone(&lifecycle);
    let exclusive_thread = std::thread::spawn(move || {
        started_tx.send(()).unwrap();
        let guard = lifecycle_for_exclusive.begin_exclusive_access().unwrap();
        exclusive_tx.send(guard).unwrap();
    });

    started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    for _ in 0..1000 {
        if lifecycle.access_mode.load(Ordering::Acquire) != ACCESS_OPEN {
            break;
        }
        std::thread::yield_now();
    }
    assert!(exclusive_rx.try_recv().is_err());
    drop(normal);
    let exclusive = exclusive_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(lifecycle.begin_access().is_err());
    drop(exclusive);
    exclusive_thread.join().unwrap();
    assert!(lifecycle.begin_access().is_ok());

    let exclusive = lifecycle.begin_exclusive_access().unwrap();
    let lifecycle_for_close = Arc::clone(&lifecycle);
    let (closed_tx, closed_rx) = mpsc::channel();
    let close_thread = std::thread::spawn(move || {
        lifecycle_for_close.begin_close().unwrap();
        lifecycle_for_close.wait_for_accesses_to_drain();
        lifecycle_for_close.mark_closed();
        closed_tx.send(()).unwrap();
    });
    assert!(closed_rx.recv_timeout(Duration::from_millis(20)).is_err());
    drop(exclusive);
    closed_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    close_thread.join().unwrap();
}
