use super::*;
use crate::file::deletion_probe::probe;
use crate::{Config, VolumeDescriptor, VolumeUsageKind};
use size::Size;
use std::sync::Arc;
use std::sync::mpsc;
use std::time::Duration;

fn config(root: &std::path::Path) -> Config {
    Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root.display())),
        total_buckets: 1,
        ..Config::default()
    }
}

#[test]
fn close_waits_for_deletions_and_serializes_concurrent_callers() {
    let root = tempfile::tempdir().unwrap();
    let config = Config {
        total_buckets: 2,
        ..config(root.path())
    };
    let a = Arc::new(Db::open(config.clone(), vec![0..=0]).unwrap());
    let b = Db::open(config, vec![1..=1]).unwrap();
    let (fs, started, release) = probe(Ok(()));
    a.file_manager
        .meta_volume
        .deletions
        .submit(fs, "blocked".into());
    started.recv_timeout(Duration::from_secs(5)).unwrap();
    let (done, results) = mpsc::channel();
    let callers: Vec<_> = (0..2)
        .map(|_| {
            let db = Arc::clone(&a);
            let done = done.clone();
            std::thread::spawn(move || done.send(db.close()).unwrap())
        })
        .collect();
    // B shares the storage root, but must not wait for A's cleanup.
    b.close().unwrap();
    assert!(results.recv_timeout(Duration::from_millis(50)).is_err());
    release.send(()).unwrap();
    for _ in 0..2 {
        results
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap();
    }
    for caller in callers {
        caller.join().unwrap();
    }
    a.close().unwrap();
}

#[test]
fn close_never_reports_success_after_a_delete_failure() {
    let root = tempfile::tempdir().unwrap();
    let db = Db::open(config(root.path()), vec![0..=0]).unwrap();
    let (fs, started, release) = probe(Err(Error::IoError("injected deletion failure".into())));
    db.file_manager
        .meta_volume
        .deletions
        .submit(fs, "failed".into());
    started.recv_timeout(Duration::from_secs(5)).unwrap();
    release.send(()).unwrap();
    assert!(
        db.close()
            .unwrap_err()
            .to_string()
            .contains("injected deletion failure")
    );
    assert!(db.close().is_err());
}

#[test]
fn forced_close_is_not_a_successful_handoff() {
    let root = tempfile::tempdir().unwrap();
    let db = Db::open(config(root.path()), vec![0..=0]).unwrap();
    db.force_close();
    assert!(db.close().unwrap_err().to_string().contains("force-closed"));
    assert!(db.close().is_err());
}

#[test]
fn shutdown_failure_keeps_its_original_error_on_repeated_close() {
    let root = tempfile::tempdir().unwrap();
    let db = Db::open(config(root.path()), vec![0..=0]).unwrap();
    db.db_lifecycle
        .mark_error(Error::IoError("injected shutdown failure".into()));
    for _ in 0..2 {
        assert!(
            db.close()
                .unwrap_err()
                .to_string()
                .contains("injected shutdown failure")
        );
    }
}

#[test]
fn closed_db_releases_primary_before_resume_even_without_drop() {
    let root = tempfile::tempdir().unwrap();
    let primary = root.path().join("primary");
    let durable = root.path().join("durable");
    let config = Config {
        volumes: vec![
            VolumeDescriptor::new(
                format!("file://{}", primary.display()),
                vec![VolumeUsageKind::PrimaryDataPriorityHigh],
            ),
            VolumeDescriptor::new(
                format!("file://{}", durable.display()),
                vec![VolumeUsageKind::Snapshot, VolumeUsageKind::Meta],
            ),
        ],
        memtable_capacity: Size::from_const(256),
        value_separation_threshold: Some(Size::from_const(16)),
        ..config(root.path())
    };
    let old = Db::open(config.clone(), vec![0..=0]).unwrap();
    let value = vec![b'x'; 128];
    for i in 0..24 {
        old.put(0, format!("key-{i}").as_bytes(), 0, &value)
            .unwrap();
    }
    let snapshot = old.create_snapshot_and_wait("close barrier test").unwrap();
    let id = old.id().to_owned();
    let data_dir = primary.join(&id).join("data");
    assert!(std::fs::read_dir(&data_dir).unwrap().next().is_some());
    old.close().unwrap();
    // No sleeps, retries, or drop(old): close itself must finish releasing runtime files.
    assert!(std::fs::read_dir(&data_dir).unwrap().next().is_none());
    let resumed = Db::resume_from_snapshot(config, snapshot, id).unwrap();
    drop(old);
    for i in 0..24 {
        assert_eq!(
            resumed
                .get(0, format!("key-{i}").as_bytes())
                .unwrap()
                .unwrap()[0]
                .as_deref(),
            Some(value.as_slice())
        );
    }
    resumed.close().unwrap();
    assert!(std::fs::read_dir(&data_dir).unwrap().next().is_none());
}

#[test]
fn cancelled_close_does_not_wait_again_on_drop() {
    for wait in [
        DeletionWait::NoWait,
        DeletionWait::Timeout(Duration::from_millis(20)),
    ] {
        let root = tempfile::tempdir().unwrap();
        let db = Db::open(config(root.path()), vec![0..=0]).unwrap();
        let queue = Arc::clone(&db.file_manager.meta_volume.deletions);
        let (fs, started, release) = probe(Ok(()));
        queue.submit(fs, "blocked".into());
        started.recv_timeout(Duration::from_secs(5)).unwrap();
        let (done, result) = mpsc::channel();
        let closer = std::thread::spawn(move || {
            let status = db.close_with_deletion_wait(wait);
            drop(db);
            done.send(status).unwrap();
        });
        let status = result.recv_timeout(Duration::from_secs(5));
        // Always release the backend, including when an incorrect Drop blocked the closer.
        release.send(()).unwrap();
        closer.join().unwrap();
        assert_eq!(
            status.unwrap().unwrap(),
            DeletionStatus::Cancelled { in_flight: true }
        );
        assert_eq!(
            queue.wait(DeletionWait::Forever).unwrap(),
            DeletionStatus::Cancelled { in_flight: false }
        );
    }
}

#[test]
fn close_after_timeout_still_waits_for_in_flight_io() {
    let root = tempfile::tempdir().unwrap();
    let db = Arc::new(Db::open(config(root.path()), vec![0..=0]).unwrap());
    let (fs, started, release) = probe(Ok(()));
    db.file_manager
        .meta_volume
        .deletions
        .submit(fs, "blocked".into());
    started.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(
        db.close_with_deletion_wait(DeletionWait::Timeout(Duration::from_millis(20)))
            .unwrap(),
        DeletionStatus::Cancelled { in_flight: true }
    );
    let (done, result) = mpsc::channel();
    let closer = std::thread::spawn(move || done.send(db.close()).unwrap());
    assert!(result.recv_timeout(Duration::from_millis(50)).is_err());
    release.send(()).unwrap();
    result
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    closer.join().unwrap();
}
