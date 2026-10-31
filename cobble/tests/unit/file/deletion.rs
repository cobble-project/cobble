use super::*;
use crate::file::deletion_probe::probe;
use std::sync::mpsc;
use std::time::Duration;

fn start_blocked_deletes(queue: &Arc<FileDeletionQueue>, count: usize) -> Vec<mpsc::Sender<()>> {
    (0..count)
        .map(|index| {
            let (fs, started, release) = probe(Ok(()));
            queue.submit(fs, format!("running-{index}"));
            started.recv_timeout(Duration::from_secs(5)).unwrap();
            release
        })
        .collect()
}

// Prepare the scheduler's initial state without spawning it, so cancellation boundaries
// can be tested deterministically before dispatch or on a deliberately occupied I/O pool.
fn queued_delete(fs: Arc<dyn FileSystem>) -> Arc<FileDeletionQueue> {
    Arc::new(FileDeletionQueue {
        state: Mutex::new(State {
            jobs: VecDeque::from([DeleteJob {
                fs,
                path: "queued".into(),
            }]),
            draining: true,
            ..State::default()
        }),
        ..FileDeletionQueue::default()
    })
}

#[test]
fn deletion_barrier_is_per_owner_and_waits_for_backend_completion() {
    let a = Arc::new(FileDeletionQueue::default());
    let b = Arc::new(FileDeletionQueue::default());
    let (fs_a, started_a, release_a) = probe(Ok(()));
    let (fs_b, started_b, release_b) = probe(Ok(()));
    a.submit(fs_a, "a".into());
    started_a.recv_timeout(Duration::from_secs(5)).unwrap();
    b.submit(fs_b, "b".into());
    started_b.recv_timeout(Duration::from_secs(5)).unwrap();
    release_b.send(()).unwrap();
    assert_eq!(
        b.wait(DeletionWait::Forever).unwrap(),
        DeletionStatus::Complete
    );
    let (done, result) = mpsc::channel();
    let waiter = std::thread::spawn(move || done.send(a.wait(DeletionWait::Forever)).unwrap());
    assert!(result.recv_timeout(Duration::from_millis(50)).is_err());
    release_a.send(()).unwrap();
    assert_eq!(
        result
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap(),
        DeletionStatus::Complete
    );
    waiter.join().unwrap();
}

#[test]
fn deletion_failure_is_returned_by_every_barrier() {
    let queue = Arc::new(FileDeletionQueue::default());
    let (fs, started, release) = probe(Err(Error::IoError("injected deletion failure".into())));
    queue.submit(fs, "failed".into());
    started.recv_timeout(Duration::from_secs(5)).unwrap();
    release.send(()).unwrap();
    assert!(
        queue
            .wait(DeletionWait::Forever)
            .unwrap_err()
            .to_string()
            .contains("injected deletion failure")
    );
    assert!(queue.wait(DeletionWait::Forever).is_err());
}

#[test]
fn cancellation_skips_queued_and_future_deletes_but_tracks_started_io() {
    for wait in [
        DeletionWait::NoWait,
        DeletionWait::Timeout(Duration::ZERO),
        DeletionWait::Timeout(Duration::from_millis(20)),
    ] {
        let queue = Arc::new(FileDeletionQueue::default());
        let releases = start_blocked_deletes(&queue, MAX_CONCURRENT_DELETIONS);
        let (queued, queued_started, _queued_release) = probe(Ok(()));
        queue.submit(queued, "queued".into());
        let waiting_since = std::time::Instant::now();
        assert_eq!(
            queue.wait(wait).unwrap(),
            DeletionStatus::Cancelled { in_flight: true }
        );
        if let DeletionWait::Timeout(timeout) = wait {
            assert!(waiting_since.elapsed() >= timeout);
        }
        assert_eq!(queue.wait_on_drop(), DeletionWait::NoWait);
        let (later, later_started, _later_release) = probe(Ok(()));
        queue.submit(later, "late drop".into());
        for release in releases {
            release.send(()).unwrap();
        }
        assert_eq!(
            queue.wait(DeletionWait::Forever).unwrap(),
            DeletionStatus::Cancelled { in_flight: false }
        );
        assert!(queued_started.try_recv().is_err());
        assert!(later_started.try_recv().is_err());
    }
}

#[test]
fn cancellation_before_runner_starts_is_already_quiescent() {
    let (fs, started, _release) = probe(Ok(()));
    let queue = queued_delete(fs);
    assert_eq!(
        queue.wait(DeletionWait::NoWait).unwrap(),
        DeletionStatus::Cancelled { in_flight: false }
    );
    let runtime = read_ahead_runtime();
    runtime.block_on(queue.run(runtime.clone()));
    assert!(started.try_recv().is_err());
}

#[test]
fn bounded_wait_can_finish_without_cancellation() {
    let queue = Arc::new(FileDeletionQueue::default());
    let (fs, started, release) = probe(Ok(()));
    queue.submit(fs, "complete".into());
    started.recv_timeout(Duration::from_secs(5)).unwrap();
    release.send(()).unwrap();
    assert_eq!(
        queue
            .wait(DeletionWait::Timeout(Duration::from_secs(5)))
            .unwrap(),
        DeletionStatus::Complete
    );
}

#[test]
fn backend_panic_finishes_the_barrier_with_an_error() {
    let queue = Arc::new(FileDeletionQueue::default());
    let (fs, started, release) = probe(Ok(()));
    queue.submit(fs, "panicking backend".into());
    started.recv_timeout(Duration::from_secs(5)).unwrap();
    // The probe panics when its release channel disconnects, like a panicking custom backend.
    drop(release);
    let error = queue.wait(DeletionWait::Forever).unwrap_err();
    assert!(error.to_string().contains("File deletion panicked"));
    assert!(queue.wait(DeletionWait::Forever).is_err());
}

#[test]
fn parallel_deletion_is_bounded_and_refills_freed_slots() {
    let queue = Arc::new(FileDeletionQueue::default());
    let mut starts = Vec::new();
    let mut releases = Vec::new();
    for index in 0..MAX_CONCURRENT_DELETIONS * 2 {
        let (fs, started, release) = probe(Ok(()));
        queue.submit(fs, format!("file-{index}"));
        starts.push(started);
        releases.push(release);
    }
    // All four must enter the backend without waiting for any earlier delete to finish.
    for started in &starts[..MAX_CONCURRENT_DELETIONS] {
        started.recv_timeout(Duration::from_secs(5)).unwrap();
    }
    for started in &starts[MAX_CONCURRENT_DELETIONS..] {
        assert!(started.try_recv().is_err());
    }
    releases[0].send(()).unwrap();
    starts[MAX_CONCURRENT_DELETIONS]
        .recv_timeout(Duration::from_secs(5))
        .unwrap();
    for release in releases.into_iter().skip(1) {
        release.send(()).unwrap();
    }
    assert_eq!(
        queue.wait(DeletionWait::Forever).unwrap(),
        DeletionStatus::Complete
    );
}

#[test]
fn same_file_is_serialized_without_blocking_other_files() {
    let queue = Arc::new(FileDeletionQueue::default());
    let (fs, started, release) = probe(Ok(()));
    queue.submit(Arc::clone(&fs), "same-path".into());
    started.recv_timeout(Duration::from_secs(5)).unwrap();
    queue.submit(fs, "same-path".into());
    let (other_fs, other_started, other_release) = probe(Ok(()));
    queue.submit(other_fs, "other-path".into());
    other_started.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(started.recv_timeout(Duration::from_millis(50)).is_err());
    other_release.send(()).unwrap();
    release.send(()).unwrap();
    started.recv_timeout(Duration::from_secs(5)).unwrap();
    release.send(()).unwrap();
    assert_eq!(
        queue.wait(DeletionWait::Forever).unwrap(),
        DeletionStatus::Complete
    );
}

#[test]
fn cancellation_skips_a_delete_already_dispatched_to_a_busy_pool() {
    // A private test pool makes the dispatched/not-started boundary deterministic without
    // saturating the production shared pool or affecting other tests.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .max_blocking_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let (occupied, occupied_rx) = mpsc::channel();
    let (unblock, unblock_rx) = mpsc::channel();
    let blocker = runtime.spawn_blocking(move || {
        occupied.send(()).unwrap();
        unblock_rx.recv().unwrap();
    });
    occupied_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let (fs, started, _release) = probe(Ok(()));
    let queue = queued_delete(fs);
    let runner = runtime.spawn(Arc::clone(&queue).run(runtime.handle().clone()));
    let dispatched = runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if queue.state.lock().unwrap().jobs.is_empty() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
    });
    let status = queue.wait(DeletionWait::NoWait);
    unblock.send(()).unwrap();
    runtime.block_on(async {
        blocker.await.unwrap();
        runner.await.unwrap();
    });
    dispatched.unwrap();
    assert_eq!(
        status.unwrap(),
        DeletionStatus::Cancelled { in_flight: false }
    );
    assert!(started.try_recv().is_err());
}

#[test]
fn cancelled_barrier_waits_for_every_started_delete() {
    let queue = Arc::new(FileDeletionQueue::default());
    let mut releases = start_blocked_deletes(&queue, MAX_CONCURRENT_DELETIONS);
    assert_eq!(
        queue.wait(DeletionWait::NoWait).unwrap(),
        DeletionStatus::Cancelled { in_flight: true }
    );
    let last = releases.pop().unwrap();
    let (done, completed) = mpsc::channel();
    let waiter = std::thread::spawn(move || done.send(queue.wait(DeletionWait::Forever)).unwrap());
    for release in releases {
        release.send(()).unwrap();
    }
    let early = completed.recv_timeout(Duration::from_millis(50));
    last.send(()).unwrap();
    assert!(
        early.is_err(),
        "one unfinished backend delete must keep the barrier pending"
    );
    assert_eq!(
        completed
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap(),
        DeletionStatus::Cancelled { in_flight: false }
    );
    waiter.join().unwrap();
}

#[test]
fn cancellation_does_not_hide_an_in_flight_deletion_failure() {
    let queue = Arc::new(FileDeletionQueue::default());
    let (fs, started, release) = probe(Err(Error::IoError("in-flight failure".into())));
    queue.submit(fs, "failed".into());
    started.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(
        queue.wait(DeletionWait::NoWait).unwrap(),
        DeletionStatus::Cancelled { in_flight: true }
    );
    release.send(()).unwrap();
    for wait in [DeletionWait::Forever, DeletionWait::NoWait] {
        assert!(
            queue
                .wait(wait)
                .unwrap_err()
                .to_string()
                .contains("in-flight failure")
        );
    }
}
