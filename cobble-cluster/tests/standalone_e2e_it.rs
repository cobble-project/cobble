use cobble::{Config, CoordinatorConfig, ReadOptions, VolumeDescriptor, VolumeUsageKind};
use cobble_cluster::{StandaloneCoordinator, StandaloneShardNode};
use std::thread;
use std::time::Duration;
use std::time::Instant;
use uuid::Uuid;

fn test_root(prefix: &str) -> String {
    format!("/tmp/{}_{}", prefix, Uuid::new_v4())
}

fn free_addr() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind random addr");
    let addr = listener.local_addr().expect("local addr");
    drop(listener);
    addr.to_string()
}

fn write_bulk(
    shard: &StandaloneShardNode,
    bucket_start: u16,
    key_prefix: &str,
    value_prefix: &str,
    start: usize,
    count: usize,
) {
    for i in start..(start + count) {
        let key = format!("{key_prefix}-{i:05}");
        let value = format!("{value_prefix}-{i:05}-{}", "x".repeat(192));
        let bucket = bucket_start + (i as u16 % 2);
        shard
            .db()
            .put(bucket, key.as_bytes(), 0, value.as_bytes())
            .expect("bulk put");
    }
}

fn assert_sample_value(shard: &StandaloneShardNode, bucket: u16, key: &str, expected: &str) {
    let value = shard
        .db()
        .get(bucket, key.as_bytes(), &ReadOptions::default())
        .expect("get value")
        .expect("value exists");
    assert_eq!(
        value[0].as_ref().expect("column 0").as_ref(),
        expected.as_bytes()
    );
}

fn take_global_checkpoint_with_retry(
    coordinator: &StandaloneCoordinator,
    total_buckets: u32,
    expected_shards: usize,
    request_timeout: Duration,
    timeout: Duration,
) -> u64 {
    let deadline = Instant::now() + timeout;
    while Instant::now() <= deadline {
        match coordinator.take_global_checkpoint(total_buckets, request_timeout) {
            Ok(snapshot_id) => {
                if let Ok(Some(manifest)) = coordinator.load_current_global_snapshot() {
                    if manifest.id == snapshot_id
                        && manifest.shard_snapshots.len() == expected_shards
                    {
                        return snapshot_id;
                    }
                }
                thread::sleep(Duration::from_millis(25));
            }
            Err(_) => thread::sleep(Duration::from_millis(25)),
        }
    }
    panic!(
        "timeout waiting for successful global checkpoint with {} shards",
        expected_shards
    );
}

#[test]
fn standalone_cluster_end_to_end_checkpoint_flow() {
    const INITIAL_BATCH_PER_SHARD: usize = 1500;
    const RESUMED_BATCH_PER_SHARD: usize = 500;

    let coord_root = test_root("standalone_e2e_coord");
    let shard_a_root = test_root("standalone_e2e_shard_a");
    let shard_b_root = test_root("standalone_e2e_shard_b");

    let _ = std::fs::remove_dir_all(&coord_root);
    let mut coordinator = StandaloneCoordinator::new(CoordinatorConfig {
        volumes: vec![VolumeDescriptor::new(
            format!("file://{}", coord_root),
            vec![
                VolumeUsageKind::Meta,
                VolumeUsageKind::PrimaryDataPriorityHigh,
            ],
        )],
    })
    .expect("open coordinator");
    let coordinator_addr = free_addr();
    coordinator
        .serve(&coordinator_addr)
        .expect("start coordinator");

    let config_a = Config {
        total_buckets: 4,
        volumes: VolumeDescriptor::single_volume(format!("file://{}", shard_a_root)),
        ..Config::default()
    };
    let config_b = Config {
        total_buckets: 4,
        volumes: VolumeDescriptor::single_volume(format!("file://{}", shard_b_root)),
        ..Config::default()
    };
    let mut shard_a = StandaloneShardNode::open(
        config_a.clone(),
        vec![0u16..=1u16],
        coordinator_addr.clone(),
    )
    .expect("open shard a");
    let mut shard_b = StandaloneShardNode::open(
        config_b.clone(),
        vec![2u16..=3u16],
        coordinator_addr.clone(),
    )
    .expect("open shard b");
    shard_a.serve().expect("serve shard a");
    shard_b.serve().expect("serve shard b");

    write_bulk(
        &shard_a,
        0,
        "shard-a-key",
        "shard-a-value",
        0,
        INITIAL_BATCH_PER_SHARD,
    );
    write_bulk(
        &shard_b,
        2,
        "shard-b-key",
        "shard-b-value",
        0,
        INITIAL_BATCH_PER_SHARD,
    );

    let first_checkpoint_id = take_global_checkpoint_with_retry(
        &coordinator,
        4,
        2,
        Duration::from_secs(6),
        Duration::from_secs(8),
    );
    let first_manifest = coordinator
        .load_current_global_snapshot()
        .expect("load first checkpoint")
        .expect("first checkpoint manifest exists");
    assert_eq!(first_manifest.id, first_checkpoint_id);
    assert_eq!(first_manifest.shard_snapshots.len(), 2);

    let shard_a_db_id = shard_a.db().id().to_string();
    let shard_b_db_id = shard_b.db().id().to_string();
    shard_a.shutdown().unwrap();
    shard_b.shutdown().unwrap();
    shard_a.db().close().expect("close shard a db");
    shard_b.db().close().expect("close shard b db");

    let mut shard_a = StandaloneShardNode::resume(
        config_a,
        shard_a_db_id,
        vec![0u16..=1u16],
        coordinator_addr.clone(),
    )
    .expect("resume shard a");
    let mut shard_b = StandaloneShardNode::resume(
        config_b,
        shard_b_db_id,
        vec![2u16..=3u16],
        coordinator_addr.clone(),
    )
    .expect("resume shard b");
    shard_a.serve().expect("serve resumed shard a");
    shard_b.serve().expect("serve resumed shard b");

    write_bulk(
        &shard_a,
        0,
        "shard-a-key",
        "shard-a-value",
        INITIAL_BATCH_PER_SHARD,
        RESUMED_BATCH_PER_SHARD,
    );
    write_bulk(
        &shard_b,
        2,
        "shard-b-key",
        "shard-b-value",
        INITIAL_BATCH_PER_SHARD,
        RESUMED_BATCH_PER_SHARD,
    );

    let second_checkpoint_id = take_global_checkpoint_with_retry(
        &coordinator,
        4,
        2,
        Duration::from_secs(6),
        Duration::from_secs(12),
    );
    let second_manifest = coordinator
        .load_current_global_snapshot()
        .expect("load second checkpoint")
        .expect("second checkpoint manifest exists");
    assert!(second_checkpoint_id > first_checkpoint_id);
    assert_eq!(second_manifest.id, second_checkpoint_id);
    assert_eq!(second_manifest.shard_snapshots.len(), 2);

    assert_sample_value(
        &shard_a,
        0,
        "shard-a-key-00000",
        &format!("shard-a-value-{:05}-{}", 0, "x".repeat(192)),
    );
    assert_sample_value(
        &shard_a,
        1,
        &format!(
            "shard-a-key-{:05}",
            INITIAL_BATCH_PER_SHARD + RESUMED_BATCH_PER_SHARD - 1
        ),
        &format!(
            "shard-a-value-{:05}-{}",
            INITIAL_BATCH_PER_SHARD + RESUMED_BATCH_PER_SHARD - 1,
            "x".repeat(192)
        ),
    );
    assert_sample_value(
        &shard_b,
        3,
        &format!(
            "shard-b-key-{:05}",
            INITIAL_BATCH_PER_SHARD + RESUMED_BATCH_PER_SHARD - 1
        ),
        &format!(
            "shard-b-value-{:05}-{}",
            INITIAL_BATCH_PER_SHARD + RESUMED_BATCH_PER_SHARD - 1,
            "x".repeat(192)
        ),
    );

    shard_a.shutdown().unwrap();
    shard_b.shutdown().unwrap();
    shard_a.db().close().expect("close resumed shard a db");
    shard_b.db().close().expect("close resumed shard b db");
    coordinator.shutdown().unwrap();
    let _ = std::fs::remove_dir_all(coord_root);
    let _ = std::fs::remove_dir_all(shard_a_root);
    let _ = std::fs::remove_dir_all(shard_b_root);
}
