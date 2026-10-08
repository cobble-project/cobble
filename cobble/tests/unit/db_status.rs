use super::*;
use std::sync::mpsc;
use std::time::{Duration, Instant};

impl DbLifecycle {
    fn wait_for_access_waiters(&self, expected: usize) {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if *self.access_wait_mutex.lock().unwrap() == expected {
                return;
            }
            assert!(Instant::now() < deadline, "drain waiters did not register");
            std::thread::yield_now();
        }
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
    let (done_tx, done_rx) = mpsc::channel();
    let handle = std::thread::spawn(move || {
        lifecycle_for_thread.begin_close().unwrap();
        lifecycle_for_thread.wait_for_accesses_to_drain();
        lifecycle_for_thread.mark_closed();
        done_tx.send(()).unwrap();
    });

    lifecycle.wait_for_access_waiters(1);
    assert!(done_rx.try_recv().is_err());

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
    let (exclusive_tx, exclusive_rx) = mpsc::channel();
    let lifecycle_for_exclusive = Arc::clone(&lifecycle);
    let exclusive_thread = std::thread::spawn(move || {
        let guard = lifecycle_for_exclusive.begin_exclusive_access().unwrap();
        exclusive_tx.send(guard).unwrap();
    });

    lifecycle.wait_for_access_waiters(1);
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
    lifecycle.wait_for_access_waiters(1);
    assert!(closed_rx.try_recv().is_err());
    drop(exclusive);
    closed_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    close_thread.join().unwrap();
}

#[test]
fn release_without_waiters_never_needs_the_wait_mutex_and_registration_rechecks_count() {
    let lifecycle = Arc::new(DbLifecycle::new_open());
    let access = lifecycle.begin_owned_access().unwrap();
    let mut waiters = lifecycle.access_wait_mutex.lock().unwrap();
    let (done_tx, done_rx) = mpsc::channel();
    let release = std::thread::spawn(move || {
        drop(access);
        done_tx.send(()).unwrap();
    });
    // Holding the mutex throughout proves the ordinary release does not lock it.
    done_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    release.join().unwrap();
    assert_eq!(lifecycle.active_access_count(), 0);
    // Model the release between a drain waiter's fast check and its registration RMW.
    lifecycle.register_access_waiter(&mut waiters);
    assert_eq!(lifecycle.active_access_count(), 0);
    assert_eq!(
        lifecycle.active_accesses.load(Ordering::Acquire),
        ACCESS_WAITERS
    );
    lifecycle.unregister_access_waiter(&mut waiters);
    assert_eq!(lifecycle.active_accesses.load(Ordering::Acquire), 0);
}

#[test]
fn registered_release_handshakes_with_the_wait_mutex_before_notifying() {
    let lifecycle = Arc::new(DbLifecycle::new_open());
    let access = lifecycle.begin_owned_access().unwrap();
    let mut waiters = lifecycle.access_wait_mutex.lock().unwrap();
    lifecycle.register_access_waiter(&mut waiters);
    let (done_tx, done_rx) = mpsc::channel();
    let release = std::thread::spawn(move || {
        drop(access);
        done_tx.send(()).unwrap();
    });
    let deadline = Instant::now() + Duration::from_secs(5);
    while lifecycle.active_access_count() != 0 {
        assert!(Instant::now() < deadline, "release did not decrement count");
        std::thread::yield_now();
    }
    // fetch_sub completed, but its notification cannot finish while we hold the wait mutex.
    assert!(done_rx.try_recv().is_err());
    let (mut waiters, wakeup) = lifecycle
        .access_wait_condvar
        .wait_timeout(waiters, Duration::from_secs(5))
        .unwrap();
    assert!(
        !wakeup.timed_out(),
        "notification was lost while entering wait"
    );
    lifecycle.unregister_access_waiter(&mut waiters);
    drop(waiters);
    done_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    release.join().unwrap();
    assert_eq!(lifecycle.active_accesses.load(Ordering::Acquire), 0);
}

#[test]
fn exclusive_drain_preserves_the_marker_for_remaining_close_waiters() {
    let lifecycle = Arc::new(DbLifecycle::new_open());
    let normal = lifecycle.begin_owned_access().unwrap();
    let sentinel = lifecycle.begin_owned_access().unwrap();
    let exclusive_lifecycle = Arc::clone(&lifecycle);
    let (exclusive_done_tx, exclusive_done_rx) = mpsc::channel();
    let exclusive_waiter = std::thread::spawn(move || {
        exclusive_lifecycle.wait_for_other_accesses_to_drain();
        exclusive_done_tx.send(()).unwrap();
    });
    let (close_done_tx, close_done_rx) = mpsc::channel();
    let close_waiters: Vec<_> = (0..2)
        .map(|_| {
            let lifecycle = Arc::clone(&lifecycle);
            let done_tx = close_done_tx.clone();
            std::thread::spawn(move || {
                lifecycle.wait_for_accesses_to_drain();
                done_tx.send(()).unwrap();
            })
        })
        .collect();
    lifecycle.wait_for_access_waiters(3);
    assert_eq!(lifecycle.active_access_count(), 2);
    drop(normal);
    exclusive_done_rx
        .recv_timeout(Duration::from_secs(5))
        .unwrap();
    exclusive_waiter.join().unwrap();
    assert_eq!(*lifecycle.access_wait_mutex.lock().unwrap(), 2);
    assert_eq!(
        lifecycle.active_accesses.load(Ordering::Acquire),
        ACCESS_WAITERS | 1
    );
    assert!(close_done_rx.try_recv().is_err());
    drop(sentinel);
    for waiter in close_waiters {
        close_done_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        waiter.join().unwrap();
    }
    assert_eq!(*lifecycle.access_wait_mutex.lock().unwrap(), 0);
    assert_eq!(lifecycle.active_accesses.load(Ordering::Acquire), 0);
}

#[test]
fn exclusive_reopen_wakes_queued_access_on_drop_and_failed_drain() {
    for fail_drain in [false, true] {
        let lifecycle = Arc::new(DbLifecycle::new_open());
        let normal = lifecycle.begin_owned_access().unwrap();
        let exclusive_lifecycle = Arc::clone(&lifecycle);
        let (exclusive_tx, exclusive_rx) = mpsc::channel();
        let exclusive_thread = std::thread::spawn(move || {
            exclusive_tx
                .send(exclusive_lifecycle.begin_exclusive_access())
                .unwrap();
        });
        lifecycle.wait_for_access_waiters(1);
        // Register an exclusive queue with the same lock/predicate protocol as begin_exclusive_access.
        let queued_lifecycle = Arc::clone(&lifecycle);
        let (queued_tx, queued_rx) = mpsc::channel();
        let (result_tx, result_rx) = mpsc::channel();
        let queued_thread = std::thread::spawn(move || {
            let wait_guard = queued_lifecycle.exclusive_wait_mutex.lock().unwrap();
            assert_ne!(
                queued_lifecycle.access_mode.load(Ordering::Acquire),
                ACCESS_OPEN
            );
            queued_tx.send(()).unwrap();
            let wait_guard = queued_lifecycle
                .exclusive_wait_condvar
                .wait_while(wait_guard, |_| {
                    queued_lifecycle.access_mode.load(Ordering::Acquire) != ACCESS_OPEN
                })
                .unwrap();
            drop(wait_guard);
            result_tx
                .send(queued_lifecycle.begin_exclusive_access())
                .unwrap();
        });
        queued_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        if fail_drain {
            lifecycle.begin_close().unwrap();
        }
        drop(normal);
        let exclusive = exclusive_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        if fail_drain {
            assert!(exclusive.is_err());
        } else {
            drop(exclusive.unwrap());
        }
        let queued = result_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(queued.is_err(), fail_drain);
        drop(queued);
        exclusive_thread.join().unwrap();
        queued_thread.join().unwrap();
        assert_eq!(lifecycle.access_mode.load(Ordering::Acquire), ACCESS_OPEN);
        assert_eq!(lifecycle.active_accesses.load(Ordering::Acquire), 0);
    }
}
