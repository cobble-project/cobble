use bytes::Bytes;
use cobble::paths::bucket_snapshot_manifest_path;
use cobble::{
    CompactionPolicyKind, Config, Db, MetricValue, ReadOptions, ScanOptions, VolumeDescriptor,
};
use std::path::Path;

const BASE_KEYS: usize = 6000;
const UPDATED_START: usize = 2000;
const UPDATED_END: usize = 5200;
const MEMTABLE_ONLY_START: usize = BASE_KEYS;
const MEMTABLE_ONLY_END: usize = BASE_KEYS + 80;
const DEFAULT_COL1: &[u8] = b"default-col1";

fn cleanup_test_root(path: &str) {
    let _ = std::fs::remove_dir_all(path);
}

fn wait_for_manifest_in_db(root: &str, db_id: &str, snapshot_id: u64) {
    let full_path = format!(
        "{}/{}",
        root,
        bucket_snapshot_manifest_path(db_id, snapshot_id)
    );
    for _ in 0..80 {
        if Path::new(&full_path).exists() {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(25));
    }
    assert!(
        Path::new(&full_path).exists(),
        "snapshot manifest missing at {}",
        full_path
    );
}

fn wait_for_compaction_in_db(db: &Db) {
    let db_id = db.id().to_string();
    for _ in 0..240 {
        let compaction_happened = db.metrics().iter().any(|sample| {
            sample.name == "compaction_write_bytes_total"
                && sample
                    .labels
                    .iter()
                    .any(|(key, value)| key == "db_id" && value == &db_id)
                && matches!(sample.value, MetricValue::Counter(v) if v > 0)
        });
        if compaction_happened {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(25));
    }
    panic!("expected compaction_write_bytes_total > 0");
}

fn key(i: usize) -> Vec<u8> {
    format!("k{:06}", i).into_bytes()
}

fn old_col0(i: usize) -> Vec<u8> {
    format!("old-c0:{:06}", i).into_bytes()
}

fn new_col0(i: usize) -> Vec<u8> {
    format!("mem-c0:{:06}", i).into_bytes()
}

fn new_col1(i: usize) -> Vec<u8> {
    format!("mem-c1:{:06}", i).into_bytes()
}

fn open_configured_db(root: &str) -> Db {
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        memtable_capacity: 8 * 1024,
        memtable_buffer_count: 2,
        num_columns: 1,
        l0_file_limit: 2,
        write_stall_limit: None,
        l1_base_bytes: 64 * 1024,
        level_size_multiplier: 2,
        max_level: 4,
        compaction_policy: CompactionPolicyKind::RoundRobin,
        base_file_size: 32 * 1024,
        block_cache_size: 0,
        sst_bloom_filter_enabled: true,
        ..Config::default()
    };
    let total_buckets = config.total_buckets;
    let full_range = 0u16..=u16::try_from(total_buckets - 1).expect("total_buckets must fit u16");
    Db::open(config, std::iter::once(full_range).collect()).unwrap()
}

fn setup_large_schema_evolution_dataset(root: &str) -> Db {
    cleanup_test_root(root);
    let db = open_configured_db(root);

    for i in 0..BASE_KEYS {
        db.put(0, key(i), 0, old_col0(i)).unwrap();
    }
    let snapshot_id = db.snapshot().unwrap();
    wait_for_manifest_in_db(root, db.id(), snapshot_id);

    let mut schema = db.update_schema();
    schema
        .add_column(1, None, Some(Bytes::from_static(DEFAULT_COL1)))
        .unwrap();
    let _ = schema.commit();

    for i in UPDATED_START..UPDATED_END {
        if i % 2 == 0 {
            db.put(0, key(i), 0, new_col0(i)).unwrap();
        }
        db.put(0, key(i), 1, new_col1(i)).unwrap();
    }

    wait_for_compaction_in_db(&db);

    for i in MEMTABLE_ONLY_START..MEMTABLE_ONLY_END {
        db.put(0, key(i), 0, new_col0(i)).unwrap();
        db.put(0, key(i), 1, new_col1(i)).unwrap();
    }

    db
}

#[test]
#[serial_test::serial(file)]
fn test_schema_evolution_get_large_mixed_sst_memtable() {
    let root = "/tmp/schema_evolution_it_get";
    let db = setup_large_schema_evolution_dataset(root);

    for i in 0..BASE_KEYS {
        let value = db
            .get(0, &key(i), &ReadOptions::default())
            .unwrap()
            .expect("value present");
        assert_eq!(value.len(), 2);
        let expected_col0 = if (UPDATED_START..UPDATED_END).contains(&i) && i % 2 == 0 {
            new_col0(i)
        } else {
            old_col0(i)
        };
        let expected_col1 = if (UPDATED_START..UPDATED_END).contains(&i) {
            new_col1(i)
        } else {
            DEFAULT_COL1.to_vec()
        };
        assert_eq!(
            value[0].as_ref().unwrap().as_ref(),
            expected_col0.as_slice()
        );
        assert_eq!(
            value[1].as_ref().unwrap().as_ref(),
            expected_col1.as_slice()
        );
    }

    for i in MEMTABLE_ONLY_START..MEMTABLE_ONLY_END {
        let value = db
            .get(0, &key(i), &ReadOptions::default())
            .unwrap()
            .expect("value present");
        assert_eq!(value.len(), 2);
        assert_eq!(value[0].as_ref().unwrap().as_ref(), new_col0(i).as_slice());
        assert_eq!(value[1].as_ref().unwrap().as_ref(), new_col1(i).as_slice());
    }

    db.close().unwrap();
    cleanup_test_root(root);
}

#[test]
#[serial_test::serial(file)]
fn test_schema_evolution_scan_large_mixed_sst_memtable() {
    let root = "/tmp/schema_evolution_it_scan";
    let db = setup_large_schema_evolution_dataset(root);

    let mut iter = db
        .scan(
            0,
            b"k000000".as_slice()..b"k999999".as_slice(),
            &ScanOptions::default(),
        )
        .unwrap();
    let mut seen = 0usize;
    let mut checkpoints = std::collections::HashMap::new();
    let checkpoint_keys = [
        0usize,
        UPDATED_START - 1,
        UPDATED_START,
        UPDATED_START + 501,
        UPDATED_END - 1,
        MEMTABLE_ONLY_START,
        MEMTABLE_ONLY_END - 1,
    ];
    while let Some(row) = iter.next() {
        let (k, cols) = row.unwrap();
        if let Ok(s) = std::str::from_utf8(k.as_ref()) {
            let idx: usize = s[1..].parse().unwrap();
            if checkpoint_keys.contains(&idx) {
                checkpoints.insert(
                    idx,
                    (
                        cols[0].as_ref().map(|v| v.to_vec()),
                        cols[1].as_ref().map(|v| v.to_vec()),
                    ),
                );
            }
        }
        seen += 1;
    }
    assert_eq!(seen, MEMTABLE_ONLY_END);

    for idx in checkpoint_keys {
        let (c0, c1) = checkpoints.get(&idx).expect("checkpoint exists");
        let expected_col0 = if (UPDATED_START..UPDATED_END).contains(&idx) && idx % 2 == 0 {
            new_col0(idx)
        } else if idx < BASE_KEYS {
            old_col0(idx)
        } else {
            new_col0(idx)
        };
        let expected_col1 = if (UPDATED_START..UPDATED_END).contains(&idx)
            || (MEMTABLE_ONLY_START..MEMTABLE_ONLY_END).contains(&idx)
        {
            new_col1(idx)
        } else {
            DEFAULT_COL1.to_vec()
        };
        assert_eq!(c0.as_ref().unwrap().as_slice(), expected_col0.as_slice());
        assert_eq!(c1.as_ref().unwrap().as_slice(), expected_col1.as_slice());
    }

    db.close().unwrap();
    cleanup_test_root(root);
}

#[test]
#[serial_test::serial(file)]
fn test_schema_evolution_get_large_with_column_projection() {
    let root = "/tmp/schema_evolution_it_projection";
    let db = setup_large_schema_evolution_dataset(root);

    let options = ReadOptions::for_columns(vec![1, 0]);
    for i in [
        0usize,
        UPDATED_START,
        UPDATED_START + 1,
        UPDATED_END - 1,
        MEMTABLE_ONLY_START,
        MEMTABLE_ONLY_END - 1,
    ] {
        let value = db
            .get(0, &key(i), &options)
            .unwrap()
            .expect("value present");
        assert_eq!(value.len(), 2);
        let expected_col0 = if (UPDATED_START..UPDATED_END).contains(&i) && i % 2 == 0 {
            new_col0(i)
        } else if i < BASE_KEYS {
            old_col0(i)
        } else {
            new_col0(i)
        };
        let expected_col1 = if (UPDATED_START..UPDATED_END).contains(&i)
            || (MEMTABLE_ONLY_START..MEMTABLE_ONLY_END).contains(&i)
        {
            new_col1(i)
        } else {
            DEFAULT_COL1.to_vec()
        };
        assert_eq!(
            value[0].as_ref().unwrap().as_ref(),
            expected_col1.as_slice()
        );
        assert_eq!(
            value[1].as_ref().unwrap().as_ref(),
            expected_col0.as_slice()
        );
    }

    db.close().unwrap();
    cleanup_test_root(root);
}
