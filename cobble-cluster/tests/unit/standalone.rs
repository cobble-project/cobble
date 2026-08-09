use super::*;
use cobble::VolumeDescriptor;
use cobble::VolumeUsageKind;
use std::collections::BTreeMap;
use std::sync::mpsc;

fn test_server_root(prefix: &str) -> String {
    format!("/tmp/{}_{}", prefix, Uuid::new_v4())
}

fn free_addr() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind random addr");
    let addr = listener.local_addr().expect("local addr");
    drop(listener);
    addr.to_string()
}

fn build_coordinator(root: &str) -> StandaloneCoordinator {
    let _ = std::fs::remove_dir_all(root);
    StandaloneCoordinator::new(CoordinatorConfig {
        volumes: vec![VolumeDescriptor::new(
            format!("file://{}", root),
            vec![
                VolumeUsageKind::Meta,
                VolumeUsageKind::PrimaryDataPriorityHigh,
            ],
        )],
        snapshot_retention: None,
    })
    .expect("open standalone coordinator")
}

#[test]
fn standalone_shard_snapshot_roundtrip_preserves_column_family_ids() {
    let input = ShardSnapshotInput {
        ranges: vec![0u16..=3u16],
        column_family_ids: BTreeMap::from([("default".to_string(), 0), ("metrics".to_string(), 1)]),
        db_id: "db-a".to_string(),
        snapshot_id: 7,
        manifest_path: "file:///tmp/db-a".to_string(),
        timestamp_seconds: 11,
        data_size_bytes: 0,
        incremental_data_size_bytes: 0,
    };
    let snapshot = StandaloneShardSnapshot::from(input.clone());
    assert_eq!(snapshot.column_family_ids, input.column_family_ids);

    let restored = ShardSnapshotInput::from(snapshot);
    assert_eq!(restored, input);
}

#[test]
fn governance_rejects_overlap() {
    let root = test_server_root("standalone_governance_overlap");
    let mut coordinator = build_coordinator(&root);
    let addr = free_addr();
    coordinator.serve(&addr).expect("start coordinator server");

    let client = StandaloneClient::new(&addr);
    client.register_db("db-a", &[0u16..=3u16], 8).unwrap();
    let err = client.register_db("db-b", &[3u16..=5u16], 8).unwrap_err();
    assert!(matches!(err, Error::IoError(_)));

    coordinator.shutdown().unwrap();
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn coordinator_broadcasts_shard_checkpoints() {
    let coord_root = test_server_root("standalone_coordinator_checkpoint");
    let shard_a_root = test_server_root("standalone_shard_a_checkpoint");
    let shard_b_root = test_server_root("standalone_shard_b_checkpoint");

    let mut coordinator = build_coordinator(&coord_root);
    let coordinator_addr = free_addr();
    coordinator
        .serve(&coordinator_addr)
        .expect("start coordinator server");

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

    let mut shard_a =
        StandaloneShardNode::open(config_a, vec![0u16..=1u16], coordinator_addr.clone())
            .expect("open shard a node");
    let mut shard_b =
        StandaloneShardNode::open(config_b, vec![2u16..=3u16], coordinator_addr.clone())
            .expect("open shard b node");

    shard_a.serve().expect("serve shard a connector");
    shard_b.serve().expect("serve shard b connector");

    coordinator
        .wait_for_shards(2, Duration::from_secs(3))
        .expect("wait for shard registration");

    let (tx, rx) = mpsc::channel();
    coordinator
        .take_global_checkpoint_with_callback(4, Duration::from_secs(5), move |result| {
            tx.send(result).expect("send callback result");
        })
        .expect("start async checkpoint");
    let checkpoint_id = rx
        .recv_timeout(Duration::from_secs(8))
        .expect("receive checkpoint callback")
        .expect("checkpoint should succeed");

    let manifest = coordinator
        .load_current_global_snapshot()
        .expect("load current checkpoint")
        .expect("checkpoint manifest exists");

    assert_eq!(manifest.id, checkpoint_id);
    assert_eq!(manifest.shard_snapshots.len(), 2);

    shard_a.shutdown().unwrap();
    shard_b.shutdown().unwrap();
    coordinator.shutdown().unwrap();
    shard_a.db().close().expect("close shard a db");
    shard_b.db().close().expect("close shard b db");

    let _ = std::fs::remove_dir_all(coord_root);
    let _ = std::fs::remove_dir_all(shard_a_root);
    let _ = std::fs::remove_dir_all(shard_b_root);
}

#[test]
fn full_duplex_link_supports_ping_and_checkpoint() {
    let coord_root = test_server_root("standalone_full_duplex_coord");
    let shard_root = test_server_root("standalone_full_duplex_shard");

    let mut coordinator = build_coordinator(&coord_root);
    let coordinator_addr = free_addr();
    coordinator
        .serve(&coordinator_addr)
        .expect("start coordinator");

    let config = Config {
        total_buckets: 4,
        volumes: VolumeDescriptor::single_volume(format!("file://{}", shard_root)),
        ..Config::default()
    };
    let mut shard = StandaloneShardNode::open(config, vec![0u16..=3u16], coordinator_addr.clone())
        .expect("open shard node");
    shard.serve().expect("start shard connector");

    coordinator
        .wait_for_shards(1, Duration::from_secs(3))
        .expect("wait for shard");
    assert_eq!(
        coordinator
            .ping_connected_shards(Duration::from_secs(2))
            .expect("ping connected shards"),
        1
    );

    let checkpoint_id = coordinator
        .take_global_checkpoint(4, Duration::from_secs(5))
        .expect("take global checkpoint");
    let manifest = coordinator
        .load_current_global_snapshot()
        .expect("load checkpoint")
        .expect("checkpoint manifest exists");
    assert_eq!(manifest.id, checkpoint_id);
    assert_eq!(manifest.shard_snapshots.len(), 1);

    assert_eq!(
        coordinator
            .ping_connected_shards(Duration::from_secs(2))
            .expect("ping connected shards after checkpoint"),
        1
    );

    shard.shutdown().unwrap();
    coordinator.shutdown().unwrap();
    shard.db().close().expect("close shard db");
    let _ = std::fs::remove_dir_all(coord_root);
    let _ = std::fs::remove_dir_all(shard_root);
}

#[test]
fn shard_reconnects_after_coordinator_restart() {
    let coord_root = test_server_root("standalone_reconnect_coord");
    let shard_root = test_server_root("standalone_reconnect_shard");

    let mut coordinator = build_coordinator(&coord_root);
    let coordinator_addr = free_addr();
    coordinator
        .serve(&coordinator_addr)
        .expect("start coordinator");

    let config = Config {
        total_buckets: 4,
        volumes: VolumeDescriptor::single_volume(format!("file://{}", shard_root)),
        ..Config::default()
    };
    let mut shard = StandaloneShardNode::open(config, vec![0u16..=3u16], coordinator_addr.clone())
        .expect("open shard node");
    shard.serve().expect("start shard connector");

    coordinator
        .wait_for_shards(1, Duration::from_secs(3))
        .expect("initial shard connect");
    coordinator.shutdown().expect("shutdown coordinator");
    coordinator
        .serve(&coordinator_addr)
        .expect("restart coordinator");

    coordinator
        .wait_for_shards(1, Duration::from_secs(8))
        .expect("shard reconnect after restart");
    assert_eq!(
        coordinator
            .ping_connected_shards(Duration::from_secs(2))
            .expect("ping after reconnect"),
        1
    );

    shard.shutdown().unwrap();
    coordinator.shutdown().unwrap();
    shard.db().close().expect("close shard db");
    let _ = std::fs::remove_dir_all(coord_root);
    let _ = std::fs::remove_dir_all(shard_root);
}

impl StandaloneCoordinator {
    fn connected_shard_count(&self) -> usize {
        self.connected_shards
            .lock()
            .expect("connected shards lock")
            .len()
    }
    fn wait_for_shards(&self, expected: usize, timeout: Duration) -> Result<()> {
        let deadline = Instant::now() + timeout;
        while Instant::now() <= deadline {
            if self.connected_shard_count() >= expected {
                return Ok(());
            }
            std::thread::sleep(ACCEPT_POLL_DELAY);
        }
        Err(Error::IoError(format!(
            "timeout waiting for {} connected shards, got {}",
            expected,
            self.connected_shard_count()
        )))
    }
    fn ping_connected_shards(&self, timeout: Duration) -> Result<usize> {
        self.runtime.block_on(ping_connected_shards_inner(
            Arc::clone(&self.connected_shards),
            timeout,
        ))
    }
}

async fn ping_connected_shards_inner(
    connected_shards: Arc<Mutex<HashMap<String, Connection>>>,
    timeout: Duration,
) -> Result<usize> {
    let entries: Vec<(String, Connection)> = connected_shards
        .lock()
        .expect("connected shards lock")
        .iter()
        .map(|(db_id, connection)| (db_id.clone(), connection.clone()))
        .collect();

    if entries.is_empty() {
        return Ok(0);
    }

    let mut workers = Vec::with_capacity(entries.len());
    for (db_id, connection) in entries {
        workers.push(tokio::spawn(async move {
            let response = connection
                .request(StandaloneRequest::Ping, timeout)
                .await
                .map_err(|err| (db_id.clone(), err))?;
            match response {
                StandaloneResponse::Pong => Ok(db_id),
                StandaloneResponse::Error(message) => Err((db_id, Error::IoError(message))),
                _ => Err((
                    db_id,
                    Error::IoError("unexpected ping response".to_string()),
                )),
            }
        }));
    }

    let mut success = 0usize;
    let mut first_error: Option<Error> = None;
    let mut failed_db_ids = Vec::new();
    for worker in workers {
        match worker.await {
            Ok(Ok(_)) => success += 1,
            Ok(Err((db_id, err))) => {
                failed_db_ids.push(db_id);
                if first_error.is_none() {
                    first_error = Some(err);
                }
            }
            Err(_) => {
                if first_error.is_none() {
                    first_error = Some(Error::IoError("ping worker task panicked".to_string()));
                }
            }
        }
    }

    if !failed_db_ids.is_empty() {
        let mut guard = connected_shards.lock().expect("connected shards lock");
        for db_id in failed_db_ids {
            if let Some(connection) = guard.remove(&db_id) {
                connection.close();
            }
        }
    }

    if let Some(err) = first_error {
        return Err(err);
    }
    Ok(success)
}
