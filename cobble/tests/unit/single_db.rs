use super::*;
use crate::VolumeDescriptor;
use std::sync::{Arc, Barrier, mpsc};
use std::time::Duration;
use uuid::Uuid;

#[test]
fn test_single_db_resume_from_global_snapshot() {
    let root = format!("/tmp/single_db_resume_{}", Uuid::new_v4());
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        total_buckets: 1,
        num_columns: 2,
        ..Config::default()
    };
    let snapshot_id = {
        let db = SingleDb::open(config.clone()).unwrap();
        db.put(0, b"k1", 0, b"v1").unwrap();
        db.put(0, b"k2", 1, b"v2").unwrap();
        let snapshot_id = db.snapshot().unwrap();
        db.close().unwrap();
        snapshot_id
    };

    let resumed = SingleDb::resume(config, snapshot_id).unwrap();
    let row1 = resumed.get(0, b"k1").unwrap().unwrap();
    assert_eq!(row1[0].as_deref(), Some(&b"v1"[..]));
    let row2 = resumed.get(0, b"k2").unwrap().unwrap();
    assert_eq!(row2[1].as_deref(), Some(&b"v2"[..]));
    resumed.put(0, b"k3", 0, b"v3").unwrap();
    let row3 = resumed.get(0, b"k3").unwrap().unwrap();
    assert_eq!(row3[0].as_deref(), Some(&b"v3"[..]));
    resumed.close().unwrap();
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn concurrent_single_db_snapshots_do_not_regress_current_pointer() {
    const SNAPSHOT_COUNT: usize = 8;

    let root = format!("/tmp/single_db_concurrent_snapshots_{}", Uuid::new_v4());
    let db = Arc::new(
        SingleDb::open(Config {
            volumes: VolumeDescriptor::single_volume(format!("file://{root}")),
            total_buckets: 1,
            num_columns: 1,
            snapshot_retention: None,
            ..Config::default()
        })
        .unwrap(),
    );
    db.put(0, b"initial", 0, b"value").unwrap();
    let start = Arc::new(Barrier::new(SNAPSHOT_COUNT + 1));
    let snapshots = (0..SNAPSHOT_COUNT)
        .map(|_| {
            let snapshot_db = Arc::clone(&db);
            let snapshot_start = Arc::clone(&start);
            std::thread::spawn(move || {
                snapshot_start.wait();
                let (tx, rx) = mpsc::channel();
                let allocated_id = snapshot_db
                    .snapshot_with_callback(move |result| {
                        let _ = tx.send(result);
                    })
                    .unwrap();
                let manifest = rx
                    .recv_timeout(Duration::from_secs(10))
                    .expect("global snapshot callback")
                    .expect("global snapshot materialization");
                assert_eq!(manifest.id, allocated_id);
                manifest
            })
        })
        .collect::<Vec<_>>();

    start.wait();
    let mut manifests = snapshots
        .into_iter()
        .map(|snapshot| snapshot.join().expect("snapshot thread did not panic"))
        .collect::<Vec<_>>();
    manifests.sort_by_key(|manifest| manifest.id);
    assert_eq!(manifests.len(), SNAPSHOT_COUNT);
    for pair in manifests.windows(2) {
        assert!(pair[0].id < pair[1].id);
        assert!(pair[0].shard_snapshots[0].snapshot_id < pair[1].shard_snapshots[0].snapshot_id);
    }
    let current = db
        .coordinator
        .load_current_global_snapshot()
        .unwrap()
        .expect("current global snapshot");
    assert_eq!(current.id, manifests.last().unwrap().id);

    db.close().unwrap();
    let _ = std::fs::remove_dir_all(root);
}
