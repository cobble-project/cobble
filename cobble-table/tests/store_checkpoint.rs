use cobble::{
    Config, GlobalSnapshotManifest, ShardSnapshotMetadata, VolumeDescriptor, VolumeUsageKind,
};
use cobble_table::snapshot::TableSnapshotCommitter;
use cobble_table::{LogicalType, Table, TableKey, TableSchema, TableStore, Value};
use serde::{Serialize, de::DeserializeOwned};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const TEST_NAME: &str = "table_store_global_checkpoint_across_processes";
const TABLES: [&str; 2] = ["accounts", "events"];
const ROWS: i64 = 128;

#[test]
fn table_store_global_checkpoint_across_processes() {
    if let Ok(role) = std::env::var("COBBLE_CHECKPOINT_WORKER") {
        let root = PathBuf::from(std::env::var_os("COBBLE_CHECKPOINT_ROOT").unwrap());
        let shard: u16 = std::env::var("COBBLE_CHECKPOINT_SHARD")
            .unwrap()
            .parse()
            .unwrap();
        run_worker(&root, shard, &role);
        return;
    }

    let root = tempfile::tempdir().unwrap();
    let config = config(root.path());
    run_workers(root.path(), "produce");
    let reports: [[ShardSnapshotMetadata; 2]; 2] =
        std::array::from_fn(|shard| read_json(&report_path(root.path(), shard as u16)));
    let committer = TableSnapshotCommitter::open(&config, 2).unwrap();
    assert!(committer.load_current_global_snapshot().unwrap().is_none());
    for _ in 0..2 {
        assert!(
            committer
                .submit(100, reports[1][0].clone())
                .unwrap()
                .is_none()
        );
    }
    let first = committer
        .submit(100, reports[0][0].clone())
        .unwrap()
        .unwrap();
    let second = committer
        .commit_batch(
            200,
            vec![
                reports[1][1].clone(),
                reports[0][1].clone(),
                reports[0][1].clone(),
            ],
        )
        .unwrap()
        .unwrap();
    assert_ne!(first.id, 100);
    assert!(second.id > first.id);
    assert_eq!(committer.load_global_snapshot(first.id).unwrap(), first);
    assert_eq!(
        committer.load_current_global_snapshot().unwrap(),
        Some(second.clone())
    );
    write_json(&root.path().join("first.json"), &first);
    write_json(&root.path().join("second.json"), &second);
    drop(committer);

    // Reopening selects persisted global IDs; no process-local application commit state survives.
    let committer = TableSnapshotCommitter::open(&config, 2).unwrap();
    assert_eq!(committer.load_global_snapshot(first.id).unwrap(), first);
    assert_eq!(
        committer.load_current_global_snapshot().unwrap(),
        Some(second)
    );
    run_workers(root.path(), "restore-first");
    run_workers(root.path(), "advance-second");
    let follow_up = committer
        .commit_batch(
            300,
            (0..2)
                .rev()
                .map(|shard| read_json(&report_path(root.path(), shard)))
                .collect(),
        )
        .unwrap()
        .unwrap();
    write_json(&root.path().join("follow-up.json"), &follow_up);
    assert_eq!(
        committer.load_current_global_snapshot().unwrap(),
        Some(follow_up.clone())
    );
    assert_eq!(
        committer.load_global_snapshot(follow_up.id).unwrap(),
        follow_up
    );
    run_workers(root.path(), "restore-follow-up");
}

fn run_worker(root: &Path, shard: u16, role: &str) {
    let config = config(root);
    let db_id = format!("shard-{shard}");
    if role == "produce" {
        let store = TableStore::builder(config)
            .db_id(db_id)
            .bucket_ranges(vec![shard * 4..=shard * 4 + 3])
            .open()
            .unwrap();
        for name in TABLES {
            drop(store.create_table(name, schema()).unwrap());
        }
        write_rows(&store, shard, 10, 0..ROWS);
        let first = store.snapshot_and_wait().unwrap();
        write_rows(&store, shard, 20, 0..ROWS);
        let second = store.snapshot_and_wait().unwrap();
        write_rows(&store, shard, 99, 0..ROWS);
        write_rows(&store, shard, 99, 1000..1016);
        store.close().unwrap();
        write_json(&report_path(root, shard), &[first, second]);
        return;
    }

    let (filename, expected) = match role {
        "restore-first" => ("first.json", 10),
        "advance-second" => ("second.json", 20),
        "restore-follow-up" => ("follow-up.json", 30),
        _ => panic!("unknown worker role: {role}"),
    };
    let global: GlobalSnapshotManifest = read_json(&root.join(filename));
    let selected = global
        .shard_snapshots
        .iter()
        .find(|entry| entry.db_id == db_id)
        .unwrap();
    // Restore the original shard identity and storage. Reassignment/rescaling is a separate step.
    let mut store = TableStore::builder(config)
        .db_id(db_id.clone())
        .resume_from_snapshot(selected.snapshot_id)
        .unwrap();
    verify_rows(&store, shard, expected);
    if role == "advance-second" {
        let first: GlobalSnapshotManifest = read_json(&root.join("first.json"));
        let old = first
            .shard_snapshots
            .iter()
            .find(|entry| entry.db_id == db_id)
            .unwrap();
        // Verification owns and releases its table handles before either controlled restart.
        store.switch_to_snapshot(old.snapshot_id).unwrap();
        verify_rows(&store, shard, 10);
        store.switch_to_snapshot(selected.snapshot_id).unwrap();
        verify_rows(&store, shard, 20);
        write_rows(&store, shard, 30, 0..ROWS);
        let follow_up = store.snapshot_and_wait().unwrap();
        write_json(&report_path(root, shard), &follow_up);
        write_rows(&store, shard, 99, 0..ROWS);
    }
    store.close().unwrap();
}

fn config(root: &Path) -> Config {
    let storage = root.join("storage");
    std::fs::create_dir_all(&storage).unwrap();
    Config {
        volumes: vec![VolumeDescriptor::new(
            url::Url::from_directory_path(storage).unwrap().to_string(),
            vec![
                VolumeUsageKind::Meta,
                VolumeUsageKind::PrimaryDataPriorityHigh,
                VolumeUsageKind::Wal,
            ],
        )],
        total_buckets: 8,
        wal_enabled: true,
        ..Config::default()
    }
}

fn schema() -> TableSchema {
    TableSchema::builder()
        .field("id", LogicalType::int64())
        .field("value", LogicalType::int64())
        .primary_key(["id"])
        .bucket_key(["id"])
        .build()
        .unwrap()
}

fn key(table: &Table, id: i64) -> TableKey {
    let mut builder = table.key_builder();
    builder.push(Value::Int64(id));
    builder.build().unwrap()
}

fn write_rows(store: &TableStore, shard: u16, value: i64, ids: std::ops::Range<i64>) {
    for (index, name) in TABLES.into_iter().enumerate() {
        let table = store.open_table(name).unwrap();
        for id in ids.clone() {
            if key(&table, id).bucket() / 4 == shard {
                table
                    .put(&[Value::Int64(id), Value::Int64(value + index as i64)])
                    .unwrap();
            }
        }
    }
}

fn verify_rows(store: &TableStore, shard: u16, expected: i64) {
    for (index, name) in TABLES.into_iter().enumerate() {
        let table = store.open_table(name).unwrap();
        assert_eq!(table.schema(), &schema());
        for id in (0..ROWS).chain(1000..1016) {
            let key = key(&table, id);
            if key.bucket() / 4 != shard {
                continue;
            }
            let expected =
                (id < ROWS).then(|| vec![Value::Int64(id), Value::Int64(expected + index as i64)]);
            assert_eq!(
                table.get(&key).unwrap(),
                expected,
                "{name}, shard {shard}, key {id}"
            );
        }
    }
}

fn report_path(root: &Path, shard: u16) -> PathBuf {
    root.join(format!("report-{shard}.json"))
}

fn write_json(path: &Path, value: &impl Serialize) {
    std::fs::write(path, serde_json::to_vec(value).unwrap()).unwrap();
}

fn read_json<T: DeserializeOwned>(path: &Path) -> T {
    serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()
}

fn run_workers(root: &Path, role: &str) {
    let workers = (0..2)
        .map(|shard| Worker::spawn(root, shard, role))
        .collect::<Vec<_>>();
    for worker in workers {
        worker.wait();
    }
}

// Kill and reap unfinished children on any panic, including a sibling's failure or timeout.
struct Worker {
    child: Option<Child>,
    log_path: PathBuf,
}

impl Worker {
    fn spawn(root: &Path, shard: u16, role: &str) -> Self {
        let log_path = root.join(format!("{role}-{shard}.log"));
        let log = std::fs::File::create(&log_path).unwrap();
        let child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", TEST_NAME, "--nocapture"])
            .env("COBBLE_CHECKPOINT_WORKER", role)
            .env("COBBLE_CHECKPOINT_ROOT", root)
            .env("COBBLE_CHECKPOINT_SHARD", shard.to_string())
            .stdout(Stdio::from(log.try_clone().unwrap()))
            .stderr(Stdio::from(log))
            .spawn()
            .unwrap();
        Self {
            child: Some(child),
            log_path,
        }
    }

    fn wait(mut self) {
        let deadline = Instant::now() + Duration::from_secs(60);
        while self.child.as_mut().unwrap().try_wait().unwrap().is_none() {
            assert!(
                Instant::now() < deadline,
                "checkpoint worker timed out ({}):\n{}",
                self.log_path.display(),
                self.log()
            );
            // Poll process completion only; storage readiness follows successful process exit.
            std::thread::sleep(Duration::from_millis(10));
        }
        let status = self.child.take().unwrap().wait().unwrap();
        assert!(
            status.success(),
            "checkpoint worker failed ({}):\n{}",
            self.log_path.display(),
            self.log()
        );
    }

    fn log(&self) -> String {
        std::fs::read_to_string(&self.log_path)
            .unwrap_or_else(|error| format!("could not read worker log: {error}"))
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        if let Some(child) = &mut self.child {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}
