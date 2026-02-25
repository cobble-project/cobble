use std::collections::HashMap;

use cobble::paths::bucket_snapshot_manifest_path;
use cobble::{
    CompactionPolicyKind, Config, Db, MemtableType, MetricValue, ReadOptions, ReadProxy,
    ReadProxyConfig, ScanOptions, SingleNodeDb, TimeProviderKind, VolumeDescriptor, WriteBatch,
};
use serde_json::Value as JsonValue;
use std::path::Path;

fn cleanup_test_root(path: &str) {
    let _ = std::fs::remove_dir_all(path);
}

fn wait_for_manifest_in_db(root: &str, db_id: &str, snapshot_id: u64) -> String {
    let full_path = format!(
        "{}/{}",
        root,
        bucket_snapshot_manifest_path(db_id, snapshot_id)
    );
    for _ in 0..50 {
        if let Ok(contents) = std::fs::read_to_string(&full_path) {
            return contents;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    std::fs::read_to_string(full_path).expect("read manifest")
}

fn wait_for_missing(path: &str) {
    for _ in 0..50 {
        if !Path::new(path).exists() {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    assert!(!Path::new(path).exists(), "path still exists: {}", path);
}

#[test]
#[serial_test::serial(file)]
fn test_db_put_get_large_dataset() {
    let root = "/tmp/db_it_large_put_get";
    cleanup_test_root(root);
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        memtable_capacity: 4 * 1024 * 1024,
        memtable_buffer_count: 2,
        num_columns: 1,
        l0_file_limit: 2,
        write_stall_limit: None,
        l1_base_bytes: 16 * 1024 * 1024,
        level_size_multiplier: 2,
        max_level: 4,
        compaction_policy: CompactionPolicyKind::RoundRobin,
        block_cache_size: 0,
        base_file_size: 4 * 1024 * 1024,
        log_path: None,
        log_console: true,
        log_level: log::LevelFilter::Debug,
        sst_bloom_filter_enabled: true,
        ..Config::default()
    };
    let db = Db::open(config).unwrap();

    let target_bytes = 256 * 1024 * 1024;
    let value_len = 64 * 1024;
    let mut total = 0usize;
    let mut expected: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
    let mut idx = 0u32;

    while total < target_bytes {
        let key = format!("user:{:08}", idx).into_bytes();
        let value = vec![b'v'; value_len];
        db.put(&key, 0, value.clone()).expect("Put should succeed");
        expected.insert(key, value);
        total += value_len;
        idx += 1;
    }

    for (key, expected_value) in expected.iter() {
        let value = db
            .get(key, &ReadOptions::default())
            .unwrap()
            .expect("Value present");
        let col = value[0].as_ref().unwrap();
        assert_eq!(col.as_ref(), expected_value.as_slice());
    }

    cleanup_test_root(root);
}

#[test]
#[serial_test::serial(file)]
fn test_db_writebatch_get_large_dataset() {
    let root = "/tmp/db_it_large_writebatch_get";
    cleanup_test_root(root);
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        memtable_capacity: 4 * 1024 * 1024,
        memtable_buffer_count: 2,
        num_columns: 1,
        l0_file_limit: 2,
        write_stall_limit: None,
        l1_base_bytes: 16 * 1024 * 1024,
        level_size_multiplier: 2,
        max_level: 4,
        compaction_policy: CompactionPolicyKind::RoundRobin,
        block_cache_size: 0,
        base_file_size: 4 * 1024 * 1024,
        log_path: None,
        log_console: true,
        log_level: log::LevelFilter::Debug,
        sst_bloom_filter_enabled: true,
        ..Config::default()
    };
    let db = Db::open(config).unwrap();

    let target_bytes = 256 * 1024 * 1024;
    let value_len = 64 * 1024;
    let mut total = 0usize;
    let mut expected: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
    let mut idx = 0u32;

    while total < target_bytes {
        let key = format!("user:{:08}", idx).into_bytes();
        let value = vec![b'v'; value_len];
        let mut batch = WriteBatch::new();
        batch.put(&key, 0, value.clone());
        db.write_batch(batch).unwrap();
        expected.insert(key, value);
        total += value_len;
        idx += 1;
    }

    for (key, expected_value) in expected.iter() {
        let value = db
            .get(key, &ReadOptions::default())
            .unwrap()
            .expect("value present");
        let col = value[0].as_ref().unwrap();
        assert_eq!(col.as_ref(), expected_value.as_slice());
    }

    cleanup_test_root(root);
}

#[test]
#[serial_test::serial(file)]
fn test_db_put_get_large_dataset_with_separated_values() {
    let root = "/tmp/db_it_large_put_get_separated";
    cleanup_test_root(root);
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        memtable_capacity: 4 * 1024 * 1024,
        memtable_buffer_count: 2,
        num_columns: 1,
        l0_file_limit: 2,
        write_stall_limit: None,
        l1_base_bytes: 16 * 1024 * 1024,
        level_size_multiplier: 2,
        max_level: 4,
        compaction_policy: CompactionPolicyKind::RoundRobin,
        block_cache_size: 0,
        base_file_size: 4 * 1024 * 1024,
        value_separation_threshold: 1024,
        log_path: None,
        log_console: true,
        log_level: log::LevelFilter::Debug,
        sst_bloom_filter_enabled: true,
        ..Config::default()
    };
    let db = Db::open(config).unwrap();

    let target_bytes = 256 * 1024 * 1024;
    let value_len = 8 * 1024;
    let mut total = 0usize;
    let mut expected: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
    let mut idx = 0u32;

    while total < target_bytes {
        let key = format!("user:{:08}", idx).into_bytes();
        let value = vec![b's'; value_len];
        db.put(&key, 0, value.clone()).expect("Put should succeed");
        expected.insert(key, value);
        total += value_len;
        idx += 1;
    }

    for (key, expected_value) in expected.iter() {
        let value = db
            .get(key, &ReadOptions::default())
            .unwrap()
            .expect("Value present");
        let col = value[0].as_ref().unwrap();
        assert_eq!(col.as_ref(), expected_value.as_slice());
    }

    cleanup_test_root(root);
}

#[test]
#[serial_test::serial(file)]
fn test_db_writebatch_get_large_dataset_with_separated_values() {
    let root = "/tmp/db_it_large_writebatch_get_separated";
    cleanup_test_root(root);
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        memtable_capacity: 4 * 1024 * 1024,
        memtable_buffer_count: 2,
        num_columns: 1,
        l0_file_limit: 2,
        write_stall_limit: None,
        l1_base_bytes: 16 * 1024 * 1024,
        level_size_multiplier: 2,
        max_level: 4,
        compaction_policy: CompactionPolicyKind::RoundRobin,
        block_cache_size: 0,
        base_file_size: 4 * 1024 * 1024,
        value_separation_threshold: 1024,
        log_path: None,
        log_console: true,
        log_level: log::LevelFilter::Debug,
        sst_bloom_filter_enabled: true,
        ..Config::default()
    };
    let db = Db::open(config).unwrap();

    let target_bytes = 256 * 1024 * 1024;
    let value_len = 8 * 1024;
    let mut total = 0usize;
    let mut expected: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
    let mut idx = 0u32;

    while total < target_bytes {
        let key = format!("user:{:08}", idx).into_bytes();
        let value = vec![b's'; value_len];
        let mut batch = WriteBatch::new();
        batch.put(&key, 0, value.clone());
        db.write_batch(batch).unwrap();
        expected.insert(key, value);
        total += value_len;
        idx += 1;
    }

    for (key, expected_value) in expected.iter() {
        let value = db
            .get(key, &ReadOptions::default())
            .unwrap()
            .expect("value present");
        let col = value[0].as_ref().unwrap();
        assert_eq!(col.as_ref(), expected_value.as_slice());
    }

    cleanup_test_root(root);
}

#[test]
#[serial_test::serial(file)]
fn test_db_get_filters_columns() {
    let root = "/tmp/db_it_get_filter_columns";
    cleanup_test_root(root);
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        memtable_capacity: 4 * 1024,
        memtable_buffer_count: 2,
        num_columns: 3,
        block_cache_size: 0,
        sst_bloom_filter_enabled: true,
        ..Config::default()
    };
    let db = Db::open(config).unwrap();

    for i in 0..1000 {
        let key = format!("key{:04}", i).into_bytes();
        db.put(&key, 0, format!("v1:{:04}", i).into_bytes())
            .expect("Put should succeed");
        db.put(&key, 1, format!("v2:{:04}", i).into_bytes())
            .expect("Put should succeed");
        db.put(&key, 2, format!("v3:{:04}", i).into_bytes())
            .expect("Put should succeed");
    }

    let options = ReadOptions::for_columns(vec![2, 0]);
    for i in 0..1000 {
        let key = format!("key{:04}", i).into_bytes();
        let value = db
            .get(&key, &options)
            .unwrap()
            .expect("Value should present");
        assert_eq!(value.len(), 2);
        assert_eq!(
            value[0].as_ref().unwrap().as_ref(),
            format!("v3:{:04}", i).into_bytes()
        );
        assert_eq!(
            value[1].as_ref().unwrap().as_ref(),
            format!("v1:{:04}", i).into_bytes()
        );
    }

    cleanup_test_root(root);
}

#[test]
#[serial_test::serial(file)]
fn test_db_delete_with_large_values() {
    let root = "/tmp/db_it_delete_large_values";
    cleanup_test_root(root);
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        memtable_capacity: 4 * 1024,
        memtable_buffer_count: 2,
        num_columns: 1,
        block_cache_size: 0,
        sst_bloom_filter_enabled: true,
        ..Config::default()
    };
    let db = Db::open(config).unwrap();
    let payload = vec![b'd'; 64];

    db.put(b"key", 0, payload.clone()).unwrap();
    for i in 0..200 {
        let key = format!("fill{:02}", i);
        db.put(key.as_bytes(), 0, payload.clone()).unwrap();
    }
    db.delete(b"key", 0).unwrap();
    for i in 200..400 {
        let key = format!("fill{:03}", i);
        db.put(key.as_bytes(), 0, payload.clone()).unwrap();
    }

    assert!(db.get(b"key", &ReadOptions::default()).unwrap().is_none());
    cleanup_test_root(root);
}

#[test]
#[serial_test::serial(file)]
fn test_db_merge_with_large_values() {
    let root = "/tmp/db_it_merge_large_values";
    cleanup_test_root(root);
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        memtable_capacity: 4 * 1024,
        memtable_buffer_count: 2,
        num_columns: 1,
        block_cache_size: 0,
        sst_bloom_filter_enabled: true,
        ..Config::default()
    };
    let db = Db::open(config).unwrap();
    let base = vec![b'a'; 64];
    let suffix = vec![b'b'; 32];

    db.put(b"key", 0, base.clone()).unwrap();
    for i in 0..200 {
        let key = format!("fill{:02}", i);
        db.put(key.as_bytes(), 0, base.clone()).unwrap();
    }
    db.merge(b"key", 0, suffix.clone()).unwrap();
    for i in 200..400 {
        let key = format!("fill{:03}", i);
        db.put(key.as_bytes(), 0, base.clone()).unwrap();
    }

    let value = db
        .get(b"key", &ReadOptions::default())
        .unwrap()
        .expect("value present");
    let mut expected = base;
    expected.extend_from_slice(&suffix);
    assert_eq!(value[0].as_ref().unwrap().as_ref(), expected.as_slice());
    cleanup_test_root(root);
}

#[test]
#[serial_test::serial(file)]
fn test_db_ttl_put_get_with_manual_time() {
    let root = "/tmp/db_it_ttl_manual";
    cleanup_test_root(root);

    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        memtable_capacity: 4 * 1024 * 1024,
        memtable_buffer_count: 2,
        num_columns: 1,
        l0_file_limit: 2,
        write_stall_limit: None,
        l1_base_bytes: 16 * 1024 * 1024,
        level_size_multiplier: 2,
        max_level: 4,
        compaction_policy: CompactionPolicyKind::RoundRobin,
        block_cache_size: 0,
        base_file_size: 4 * 1024 * 1024,
        ttl_enabled: true,
        default_ttl_seconds: None,
        time_provider: TimeProviderKind::Manual,
        log_path: None,
        log_console: false,
        log_level: log::LevelFilter::Info,
        sst_bloom_filter_enabled: true,
        ..Config::default()
    };
    let db = Db::open(config).unwrap();

    // Put with explicit TTL of 10 seconds
    let mut batch = WriteBatch::new();
    batch.put_with_ttl(b"key", 0, b"value".to_vec(), Some(10));
    db.write_batch(batch).unwrap();

    // At t=1000, value should be visible
    let v = db.get(b"key", &ReadOptions::default()).unwrap().unwrap();
    assert_eq!(v[0].as_ref().unwrap().as_ref(), b"value");

    // Advance time past expiry
    db.set_time(1_011);
    assert!(db.get(b"key", &ReadOptions::default()).unwrap().is_none());

    cleanup_test_root(root);
}

#[test]
#[serial_test::serial(file)]
fn test_db_ttl_default_ttl_with_manual_time() {
    let root = "/tmp/db_it_ttl_default";
    cleanup_test_root(root);

    let config = Config {
        memtable_capacity: 4 * 1024 * 1024,
        memtable_buffer_count: 2,
        num_columns: 1,
        l0_file_limit: 2,
        write_stall_limit: None,
        l1_base_bytes: 16 * 1024 * 1024,
        level_size_multiplier: 2,
        max_level: 4,
        compaction_policy: CompactionPolicyKind::RoundRobin,
        block_cache_size: 0,
        base_file_size: 4 * 1024 * 1024,
        ttl_enabled: true,
        default_ttl_seconds: Some(5),
        time_provider: cobble::TimeProviderKind::Manual,
        log_path: None,
        log_console: false,
        log_level: log::LevelFilter::Info,
        sst_bloom_filter_enabled: true,
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        ..Config::default()
    };
    let db = Db::open(config).unwrap();

    // Put without explicit TTL uses default (5s)
    let mut batch = WriteBatch::new();
    batch.put(b"foo", 0, b"bar".to_vec());
    db.write_batch(batch).unwrap();

    // Before expiry
    assert!(db.get(b"foo", &ReadOptions::default()).unwrap().is_some());

    // Move to expiry boundary
    db.set_time(5_005);
    assert!(db.get(b"foo", &ReadOptions::default()).unwrap().is_none());

    cleanup_test_root(root);
}

#[test]
#[serial_test::serial(file)]
fn test_db_snapshot_creates_manifest() {
    let root = "/tmp/db_snapshot_manifest";
    cleanup_test_root(root);
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        memtable_capacity: 128,
        memtable_buffer_count: 2,
        num_columns: 1,
        block_cache_size: 0,
        sst_bloom_filter_enabled: true,
        ..Config::default()
    };
    let db = Db::open(config).unwrap();

    let mut batch = WriteBatch::new();
    batch.put(b"k1", 0, b"v1".to_vec());
    db.write_batch(batch).unwrap();

    let snapshot_id = db.snapshot().unwrap();
    let manifest = wait_for_manifest_in_db(root, db.id(), snapshot_id);
    assert!(manifest.contains("\"id\":"));
    assert!(manifest.contains("\"seq_id\":"));
    assert!(manifest.contains("\"levels\""));
    assert!(manifest.contains("\"path\":\""));

    cleanup_test_root(root);
}

#[test]
#[serial_test::serial(file)]
fn test_single_node_db_snapshot_updates_global_manifest() {
    let root = "/tmp/single_node_db_snapshot";
    cleanup_test_root(root);
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        memtable_capacity: 128,
        memtable_buffer_count: 2,
        num_columns: 1,
        block_cache_size: 0,
        sst_bloom_filter_enabled: true,
        ..Config::default()
    };
    let db = SingleNodeDb::open(config.clone(), 4).unwrap();

    db.put(b"k1", 0, b"v1".to_vec()).unwrap();
    db.put(b"k2", 0, b"v2".to_vec()).unwrap();
    db.put(b"x1", 0, b"vx".to_vec()).unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    let global_id = db
        .snapshot_with_callback(move |result| {
            let _ = tx.send(result);
        })
        .unwrap();
    let callback_id = rx
        .recv()
        .expect("snapshot callback dropped")
        .expect("snapshot materialized");
    assert_eq!(global_id, callback_id);

    let mut proxy = ReadProxy::open_current(ReadProxyConfig::from_config(&config)).unwrap();
    let value = proxy
        .get(0, b"k1", &ReadOptions::default())
        .unwrap()
        .expect("value present");
    let col = value[0].as_ref().unwrap();
    assert_eq!(col.as_ref(), b"v1");
    let mut iter = proxy
        .scan(
            0,
            b"k1".as_slice()..b"kz".as_slice(),
            &ScanOptions::default(),
        )
        .unwrap();
    let mut rows = Vec::new();
    while let Some(row) = iter.next() {
        rows.push(row.unwrap());
    }
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].0.as_ref(), b"k1");
    assert_eq!(rows[0].1[0].as_ref().unwrap().as_ref(), b"v1");
    assert_eq!(rows[1].0.as_ref(), b"k2");
    assert_eq!(rows[1].1[0].as_ref().unwrap().as_ref(), b"v2");
    cleanup_test_root(root);
}

#[test]
#[serial_test::serial(file)]
fn test_db_snapshot_read_only_get_with_vec_memtable() {
    let root = "/tmp/db_snapshot_readonly_vec_memtable";
    cleanup_test_root(root);
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        memtable_capacity: 256,
        memtable_buffer_count: 2,
        memtable_type: MemtableType::Vec,
        num_columns: 1,
        block_cache_size: 0,
        sst_bloom_filter_enabled: true,
        ..Config::default()
    };
    let db = Db::open(config.clone()).unwrap();
    let mut expected = HashMap::new();
    for i in 0..128u32 {
        let key = format!("k{:04}", i).into_bytes();
        let value = format!("value-{:04}", i).into_bytes();
        db.put(&key, 0, value.clone()).unwrap();
        expected.insert(key, value);
    }

    let snapshot_id = db.snapshot().unwrap();
    let _ = wait_for_manifest_in_db(root, db.id(), snapshot_id);
    db.close().unwrap();

    let ro = Db::open_read_only(config, snapshot_id, db.id().to_string()).unwrap();
    for (key, expected_value) in expected.iter() {
        let value = ro
            .get(key, &ReadOptions::default())
            .unwrap()
            .expect("value present");
        let col = value[0].as_ref().unwrap();
        assert_eq!(col.as_ref(), expected_value.as_slice());
    }
    cleanup_test_root(root);
}

#[test]
#[serial_test::serial(file)]
fn test_db_snapshot_read_only_get_with_vec_memtable_separated_values() {
    let root = "/tmp/db_snapshot_readonly_vec_memtable_separated";
    cleanup_test_root(root);
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        memtable_capacity: 256,
        memtable_buffer_count: 2,
        memtable_type: MemtableType::Vec,
        num_columns: 1,
        value_separation_threshold: 8,
        block_cache_size: 0,
        sst_bloom_filter_enabled: true,
        ..Config::default()
    };
    let db = Db::open(config.clone()).unwrap();
    let mut expected = HashMap::new();
    for i in 0..64u32 {
        let key = format!("k{:04}", i).into_bytes();
        let value = vec![b'a' + (i % 26) as u8; 96];
        db.put(&key, 0, value.clone()).unwrap();
        expected.insert(key, value);
    }

    let snapshot_id = db.snapshot().unwrap();
    let _ = wait_for_manifest_in_db(root, db.id(), snapshot_id);
    db.close().unwrap();

    let ro = Db::open_read_only(config, snapshot_id, db.id().to_string()).unwrap();
    for (key, expected_value) in expected.iter() {
        let value = ro
            .get(key, &ReadOptions::default())
            .unwrap()
            .expect("value present");
        let col = value[0].as_ref().unwrap();
        assert_eq!(col.as_ref(), expected_value.as_slice());
    }
    cleanup_test_root(root);
}

#[test]
#[serial_test::serial(file)]
fn test_db_snapshot_read_only_get() {
    let root = "/tmp/db_snapshot_readonly";
    cleanup_test_root(root);
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        memtable_capacity: 128,
        memtable_buffer_count: 2,
        num_columns: 1,
        block_cache_size: 0,
        sst_bloom_filter_enabled: true,
        ..Config::default()
    };
    let db = Db::open(config.clone()).unwrap();

    let mut batch = WriteBatch::new();
    batch.put(b"k1", 0, b"v1".to_vec());
    db.write_batch(batch).unwrap();

    let snapshot_id = db.snapshot().unwrap();
    let _ = wait_for_manifest_in_db(root, db.id(), snapshot_id);
    db.close().unwrap();

    let ro = Db::open_read_only(config, snapshot_id, db.id().to_string()).unwrap();
    let value = ro
        .get(b"k1", &ReadOptions::default())
        .unwrap()
        .expect("value present");
    let col = value[0].as_ref().unwrap();
    assert_eq!(col.as_ref(), b"v1");

    cleanup_test_root(root);
}

#[test]
#[serial_test::serial(file)]
fn test_db_snapshot_read_only_get_with_separated_value() {
    let root = "/tmp/db_snapshot_readonly_separated";
    cleanup_test_root(root);
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        memtable_capacity: 128,
        memtable_buffer_count: 2,
        num_columns: 1,
        value_separation_threshold: 8,
        block_cache_size: 0,
        sst_bloom_filter_enabled: true,
        ..Config::default()
    };
    let db = Db::open(config.clone()).unwrap();
    let large = b"value-larger-than-threshold";

    let mut batch = WriteBatch::new();
    batch.put(b"k1", 0, large.to_vec());
    db.write_batch(batch).unwrap();

    let snapshot_id = db.snapshot().unwrap();
    let _ = wait_for_manifest_in_db(root, db.id(), snapshot_id);
    db.close().unwrap();

    let ro = Db::open_read_only(config, snapshot_id, db.id().to_string()).unwrap();
    let value = ro
        .get(b"k1", &ReadOptions::default())
        .unwrap()
        .expect("value present");
    let col = value[0].as_ref().unwrap();
    assert_eq!(col.as_ref(), large);

    cleanup_test_root(root);
}

#[test]
#[serial_test::serial(file)]
fn test_db_snapshot_read_only_scan_with_separated_value() {
    let root = "/tmp/db_snapshot_readonly_scan_separated";
    cleanup_test_root(root);
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        memtable_capacity: 128,
        memtable_buffer_count: 2,
        num_columns: 1,
        value_separation_threshold: 8,
        block_cache_size: 0,
        sst_bloom_filter_enabled: true,
        ..Config::default()
    };
    let db = Db::open(config.clone()).unwrap();
    let huge = vec![b'h'; 128];
    db.put(b"aa", 0, b"ignore-left").unwrap();
    db.put(b"k1", 0, b"v1").unwrap();
    db.put(b"k2", 0, huge.clone()).unwrap();
    db.put(b"z1", 0, b"ignore-right").unwrap();

    let snapshot_id = db.snapshot().unwrap();
    let _ = wait_for_manifest_in_db(root, db.id(), snapshot_id);
    db.close().unwrap();

    let ro = Db::open_read_only(config, snapshot_id, db.id().to_string()).unwrap();
    let mut iter = ro
        .scan(
            0,
            b"k1".as_slice()..b"kz".as_slice(),
            &ScanOptions::default(),
        )
        .unwrap();
    let mut rows = Vec::new();
    while let Some(row) = iter.next() {
        rows.push(row.unwrap());
    }
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].0.as_ref(), b"k1");
    assert_eq!(rows[0].1[0].as_ref().unwrap().as_ref(), b"v1");
    assert_eq!(rows[1].0.as_ref(), b"k2");
    assert_eq!(rows[1].1[0].as_ref().unwrap().as_ref(), huge.as_slice());

    cleanup_test_root(root);
}

#[test]
#[serial_test::serial(file)]
fn test_db_incremental_snapshot_restore() {
    let root = "/tmp/db_snapshot_incremental_restore";
    cleanup_test_root(root);
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        memtable_capacity: 128,
        memtable_buffer_count: 2,
        num_columns: 1,
        block_cache_size: 0,
        sst_bloom_filter_enabled: true,
        ..Config::default()
    };
    let db = Db::open(config.clone()).unwrap();
    db.put(b"k1", 0, b"v1").unwrap();
    let base_snapshot_id = db.snapshot().unwrap();
    let _ = wait_for_manifest_in_db(root, db.id(), base_snapshot_id);

    db.put(b"k2", 0, b"v2").unwrap();
    let incremental_snapshot_id = db.snapshot().unwrap();
    let incremental_manifest = wait_for_manifest_in_db(root, db.id(), incremental_snapshot_id);
    let manifest_json: JsonValue = serde_json::from_str(&incremental_manifest).unwrap();
    assert_eq!(
        manifest_json
            .get("base_snapshot_id")
            .and_then(|value| value.as_u64()),
        Some(base_snapshot_id)
    );
    assert!(manifest_json.get("vlog_files").is_some());
    db.close().unwrap();

    let ro =
        Db::open_read_only(config.clone(), incremental_snapshot_id, db.id().to_string()).unwrap();
    let value1 = ro
        .get(b"k1", &ReadOptions::default())
        .unwrap()
        .expect("k1 value present");
    assert_eq!(value1[0].as_ref().unwrap().as_ref(), b"v1");
    let value2 = ro
        .get(b"k2", &ReadOptions::default())
        .unwrap()
        .expect("k2 value present");
    assert_eq!(value2[0].as_ref().unwrap().as_ref(), b"v2");

    let writable =
        Db::open_from_snapshot(config, incremental_snapshot_id, db.id().to_string()).unwrap();
    let value1 = writable
        .get(b"k1", &ReadOptions::default())
        .unwrap()
        .expect("k1 value present");
    assert_eq!(value1[0].as_ref().unwrap().as_ref(), b"v1");
    let value2 = writable
        .get(b"k2", &ReadOptions::default())
        .unwrap()
        .expect("k2 value present");
    assert_eq!(value2[0].as_ref().unwrap().as_ref(), b"v2");

    cleanup_test_root(root);
}

#[test]
#[serial_test::serial(file)]
fn test_db_incremental_snapshot_keeps_base_manifest_live() {
    let root = "/tmp/db_snapshot_incremental_dependency";
    cleanup_test_root(root);
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        memtable_capacity: 128,
        memtable_buffer_count: 2,
        num_columns: 1,
        block_cache_size: 0,
        sst_bloom_filter_enabled: true,
        ..Config::default()
    };
    let db = Db::open(config).unwrap();
    db.put(b"k1", 0, b"v1").unwrap();
    let base_snapshot_id = db.snapshot().unwrap();
    let _ = wait_for_manifest_in_db(root, db.id(), base_snapshot_id);
    db.put(b"k2", 0, b"v2").unwrap();
    let incremental_snapshot_id = db.snapshot().unwrap();
    let _ = wait_for_manifest_in_db(root, db.id(), incremental_snapshot_id);

    let base_manifest_path = format!(
        "{}/{}",
        root,
        bucket_snapshot_manifest_path(db.id(), base_snapshot_id)
    );
    assert!(db.expire_snapshot(base_snapshot_id).unwrap());
    assert!(Path::new(&base_manifest_path).exists());

    assert!(db.expire_snapshot(incremental_snapshot_id).unwrap());
    let _ = db.expire_snapshot(base_snapshot_id).unwrap();
    wait_for_missing(&base_manifest_path);

    cleanup_test_root(root);
}

#[test]
#[serial_test::serial(file)]
fn test_db_open_from_snapshot_allows_writes() {
    let root = "/tmp/db_snapshot_write";
    cleanup_test_root(root);
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        memtable_capacity: 128,
        memtable_buffer_count: 2,
        num_columns: 1,
        block_cache_size: 0,
        sst_bloom_filter_enabled: true,
        ..Config::default()
    };
    let db = Db::open(config.clone()).unwrap();

    let mut batch = WriteBatch::new();
    batch.put(b"k1", 0, b"v1".to_vec());
    db.write_batch(batch).unwrap();

    let snapshot_id = db.snapshot().unwrap();
    let _ = wait_for_manifest_in_db(root, db.id(), snapshot_id);
    db.close().unwrap();

    let writable = Db::open_from_snapshot(config, snapshot_id, db.id().to_string()).unwrap();
    let mut batch = WriteBatch::new();
    batch.put(b"k2", 0, b"v2".to_vec());
    writable.write_batch(batch).unwrap();

    let value = writable
        .get(b"k1", &ReadOptions::default())
        .unwrap()
        .expect("value present");
    let col = value[0].as_ref().unwrap();
    assert_eq!(col.as_ref(), b"v1");

    let value = writable
        .get(b"k2", &ReadOptions::default())
        .unwrap()
        .expect("value present");
    let col = value[0].as_ref().unwrap();
    assert_eq!(col.as_ref(), b"v2");

    cleanup_test_root(root);
}

#[test]
#[serial_test::serial(file)]
fn test_db_resume_takes_over_snapshot_lifecycle() {
    let root = "/tmp/db_snapshot_takeover";
    cleanup_test_root(root);
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        memtable_capacity: 128,
        memtable_buffer_count: 2,
        num_columns: 1,
        block_cache_size: 0,
        sst_bloom_filter_enabled: true,
        ..Config::default()
    };
    let db = Db::open(config.clone()).unwrap();
    db.put(b"k1", 0, b"v1").unwrap();
    let snapshot_id = db.snapshot().unwrap();
    assert!(db.retain_snapshot(snapshot_id));
    let _ = wait_for_manifest_in_db(root, db.id(), snapshot_id);
    db.close().unwrap();

    let writable =
        Db::open_from_snapshot(config.clone(), snapshot_id, db.id().to_string()).unwrap();
    assert!(!writable.expire_snapshot(snapshot_id).unwrap());
    writable.close().unwrap();

    let writable = Db::resume(config, db.id().to_string()).unwrap();
    assert!(
        writable
            .bucket_snapshot_input(snapshot_id, vec![0u16..1u16])
            .is_ok()
    );

    let manifest_path = format!(
        "{}/{}",
        root,
        bucket_snapshot_manifest_path(writable.id(), snapshot_id)
    );
    assert!(writable.expire_snapshot(snapshot_id).unwrap());
    wait_for_missing(&manifest_path);
    writable.close().unwrap();
    cleanup_test_root(root);
}

#[test]
#[serial_test::serial(file)]
fn test_db_metrics_list() {
    let root = "/tmp/db_metrics_list";
    cleanup_test_root(root);
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        memtable_capacity: 128,
        memtable_buffer_count: 2,
        num_columns: 1,
        block_cache_size: 0,
        sst_bloom_filter_enabled: true,
        ..Config::default()
    };
    let db = Db::open(config).unwrap();

    let mut batch = WriteBatch::new();
    batch.put(b"k1", 0, b"v1".to_vec());
    db.write_batch(batch).unwrap();

    let snapshot_id = db.snapshot().unwrap();
    let _ = wait_for_manifest_in_db(root, db.id(), snapshot_id);

    let metrics = db.metrics();
    let db_id = db.id();
    let matches_db = |labels: &[(String, String)]| {
        labels
            .iter()
            .any(|(key, value)| key == "db_id" && value == db_id)
    };

    let flushes = metrics
        .iter()
        .find(|sample| sample.name == "memtable_flushes_total" && matches_db(&sample.labels));
    assert!(flushes.is_some());
    if let Some(sample) = flushes {
        match &sample.value {
            MetricValue::Counter(value) => assert!(*value >= 1),
            _ => panic!("expected counter"),
        }
    }

    let bytes = metrics
        .iter()
        .find(|sample| sample.name == "memtable_flush_bytes_total" && matches_db(&sample.labels));
    assert!(bytes.is_some());
    if let Some(sample) = bytes {
        match &sample.value {
            MetricValue::Counter(value) => assert!(*value > 0),
            _ => panic!("expected counter"),
        }
    }

    cleanup_test_root(root);
}

#[test]
#[serial_test::serial(file)]
fn test_db_expire_snapshot_releases_manifest() {
    let root = "/tmp/db_snapshot_expire";
    cleanup_test_root(root);
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        memtable_capacity: 128,
        memtable_buffer_count: 2,
        num_columns: 1,
        block_cache_size: 0,
        ..Config::default()
    };
    let db = Db::open(config).unwrap();

    let mut batch = WriteBatch::new();
    batch.put(b"k1", 0, b"v1".to_vec());
    db.write_batch(batch).unwrap();

    let snapshot_id = db.snapshot().unwrap();
    let manifest_path = format!(
        "{}/{}",
        root,
        bucket_snapshot_manifest_path(db.id(), snapshot_id)
    );
    let _ = wait_for_manifest_in_db(root, db.id(), snapshot_id);

    assert!(db.expire_snapshot(snapshot_id).unwrap());
    wait_for_missing(&manifest_path);

    cleanup_test_root(root);
}

#[test]
#[serial_test::serial(file)]
fn test_db_snapshot_auto_expire() {
    let root = "/tmp/db_snapshot_auto_expire";
    cleanup_test_root(root);
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        memtable_capacity: 128,
        memtable_buffer_count: 2,
        num_columns: 1,
        block_cache_size: 0,
        snapshot_retention: Some(1),
        snapshot_on_flush: true,
        sst_bloom_filter_enabled: true,
        ..Config::default()
    };
    let db = Db::open(config).unwrap();

    let mut batch = WriteBatch::new();
    batch.put(b"k1", 0, b"v1".to_vec());
    db.write_batch(batch).unwrap();

    let first_id = db.snapshot().unwrap();
    let _ = wait_for_manifest_in_db(root, db.id(), first_id);

    let mut batch = WriteBatch::new();
    batch.put(b"k2", 0, b"v2".to_vec());
    db.write_batch(batch).unwrap();

    let second_id = db.snapshot().unwrap();
    let _ = wait_for_manifest_in_db(root, db.id(), second_id);

    let _ = wait_for_manifest_in_db(root, db.id(), first_id);
    let _ = wait_for_manifest_in_db(root, db.id(), second_id);
    assert!(db.expire_snapshot(first_id).unwrap());
    assert!(db.expire_snapshot(second_id).unwrap());
    let _ = db.expire_snapshot(first_id).unwrap();

    cleanup_test_root(root);
}

#[test]
#[serial_test::serial(file)]
fn test_db_snapshot_retain_skips_auto_expire() {
    let root = "/tmp/db_snapshot_retain_auto";
    cleanup_test_root(root);
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        memtable_capacity: 128,
        memtable_buffer_count: 2,
        num_columns: 1,
        block_cache_size: 0,
        snapshot_retention: Some(1),
        snapshot_on_flush: true,
        sst_bloom_filter_enabled: true,
        ..Config::default()
    };
    let db = Db::open(config).unwrap();

    let mut batch = WriteBatch::new();
    batch.put(b"k1", 0, b"v1".to_vec());
    db.write_batch(batch).unwrap();

    let first_id = db.snapshot().unwrap();
    let _ = wait_for_manifest_in_db(root, db.id(), first_id);
    assert!(db.retain_snapshot(first_id));

    let mut batch = WriteBatch::new();
    batch.put(b"k2", 0, b"v2".to_vec());
    db.write_batch(batch).unwrap();

    let second_id = db.snapshot().unwrap();
    let _ = wait_for_manifest_in_db(root, db.id(), second_id);

    let _ = wait_for_manifest_in_db(root, db.id(), first_id);
    let _ = wait_for_manifest_in_db(root, db.id(), second_id);
    assert!(db.expire_snapshot(first_id).unwrap());
    assert!(db.expire_snapshot(second_id).unwrap());
    let _ = db.expire_snapshot(first_id).unwrap();

    cleanup_test_root(root);
}

#[test]
#[serial_test::serial(file)]
fn test_db_scan_put_reads_from_memtable_and_sst() {
    let root = "/tmp/db_it_scan_memtable_sst";
    cleanup_test_root(root);
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        memtable_capacity: 1024,
        memtable_buffer_count: 2,
        num_columns: 1,
        value_separation_threshold: 64,
        block_cache_size: 0,
        sst_bloom_filter_enabled: true,
        ..Config::default()
    };
    let db = Db::open(config).unwrap();

    let huge_sst = vec![b's'; 512];
    let huge_mem = vec![b'm'; 768];

    db.put(b"aaa:outside", 0, b"ignore-left").unwrap();
    db.put(b"mix:key", 0, b"old").unwrap();
    db.put(b"mix:huge-sst", 0, huge_sst.clone()).unwrap();
    for i in 0..200u32 {
        let key = format!("fill:{:04}", i).into_bytes();
        db.put(&key, 0, vec![b'f'; 96]).unwrap();
    }
    db.put(b"mix:key", 0, b"new").unwrap();
    db.put(b"mix:huge-mem", 0, huge_mem.clone()).unwrap();
    db.put(b"mix:tail", 0, b"tail").unwrap();
    db.put(b"zzz:outside", 0, b"ignore-right").unwrap();

    let mut iter = db
        .scan(
            0,
            b"fill:0000".as_slice()..b"mix;".as_slice(),
            &ScanOptions::default(),
        )
        .unwrap();
    let mut seen: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
    while let Some(row) = iter.next() {
        let (key, columns) = row.unwrap();
        if let Some(value) = columns[0].as_ref() {
            seen.insert(key.to_vec(), value.to_vec());
        }
    }

    assert_eq!(seen.len(), 204);
    for i in 0..200u32 {
        let key = format!("fill:{:04}", i).into_bytes();
        assert_eq!(
            seen.get(&key).unwrap().as_slice(),
            vec![b'f'; 96].as_slice()
        );
    }
    assert_eq!(seen.get(&b"mix:key".to_vec()).unwrap().as_slice(), b"new");
    assert_eq!(
        seen.get(&b"mix:huge-sst".to_vec()).unwrap().as_slice(),
        huge_sst.as_slice()
    );
    assert_eq!(
        seen.get(&b"mix:huge-mem".to_vec()).unwrap().as_slice(),
        huge_mem.as_slice()
    );
    assert_eq!(seen.get(&b"mix:tail".to_vec()).unwrap().as_slice(), b"tail");
    assert!(!seen.contains_key(&b"aaa:outside".to_vec()));
    assert!(!seen.contains_key(&b"zzz:outside".to_vec()));

    cleanup_test_root(root);
}

#[test]
#[serial_test::serial(file)]
fn test_db_scan_put_range_keeps_latest_value() {
    let root = "/tmp/db_it_scan_range_latest";
    cleanup_test_root(root);
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        memtable_capacity: 1024,
        memtable_buffer_count: 2,
        num_columns: 1,
        block_cache_size: 0,
        sst_bloom_filter_enabled: true,
        ..Config::default()
    };
    let db = Db::open(config).unwrap();

    for i in 0..160u32 {
        let key = format!("r:{:04}", i).into_bytes();
        db.put(&key, 0, format!("v:{:04}", i).into_bytes()).unwrap();
    }
    db.put(b"q:outside", 0, b"left").unwrap();
    db.put(b"r:0050", 0, b"latest").unwrap();
    db.put(b"s:outside", 0, b"right").unwrap();

    let mut iter = db
        .scan(
            0,
            b"r:0000".as_slice()..b"r:0100".as_slice(),
            &ScanOptions::default(),
        )
        .unwrap();
    let mut seen: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
    while let Some(row) = iter.next() {
        let (key, columns) = row.unwrap();
        if let Some(value) = columns[0].as_ref() {
            seen.insert(key.to_vec(), value.to_vec());
        }
    }

    assert_eq!(seen.len(), 100);
    assert_eq!(seen.get(&b"r:0050".to_vec()).unwrap().as_slice(), b"latest");
    assert_eq!(seen.get(&b"r:0000".to_vec()).unwrap().as_slice(), b"v:0000");
    assert_eq!(seen.get(&b"r:0099".to_vec()).unwrap().as_slice(), b"v:0099");
    assert!(!seen.contains_key(&b"q:outside".to_vec()));
    assert!(!seen.contains_key(&b"s:outside".to_vec()));

    cleanup_test_root(root);
}
