use std::collections::HashMap;

use cobble::{CompactionPolicyKind, Config, Db, TimeProviderKind, WriteBatch};

fn cleanup_test_root(path: &str) {
    let _ = std::fs::remove_dir_all(path);
}

#[test]
#[serial_test::serial(file)]
fn test_db_put_get_large_dataset() {
    let root = "/tmp/db_it_large_put_get";
    cleanup_test_root(root);
    let config = Config {
        path: format!("file://{}", root),
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
        let value = db.get(key).unwrap().expect("value present");
        let col = value[0].as_ref().unwrap();
        assert_eq!(col.as_ref(), expected_value.as_slice());
    }

    cleanup_test_root(root);
}

#[test]
#[serial_test::serial(file)]
fn test_db_ttl_put_get_with_manual_time() {
    let root = "/tmp/db_it_ttl_manual";
    cleanup_test_root(root);

    let config = Config {
        path: format!("file://{}", root),
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
    };
    let db = Db::open(config).unwrap();

    // Put with explicit TTL of 10 seconds
    let mut batch = WriteBatch::new();
    batch.put_with_ttl(b"key", 0, b"value".to_vec(), Some(10));
    db.write_batch(batch).unwrap();

    // At t=1000, value should be visible
    let v = db.get(b"key").unwrap().unwrap();
    assert_eq!(v[0].as_ref().unwrap().as_ref(), b"value");

    // Advance time past expiry
    db.set_time(1_011);
    assert!(db.get(b"key").unwrap().is_none());

    cleanup_test_root(root);
}

#[test]
#[serial_test::serial(file)]
fn test_db_ttl_default_ttl_with_manual_time() {
    let root = "/tmp/db_it_ttl_default";
    cleanup_test_root(root);

    let config = Config {
        path: format!("file://{}", root),
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
    };
    let db = Db::open(config).unwrap();

    // Put without explicit TTL uses default (5s)
    let mut batch = WriteBatch::new();
    batch.put(b"foo", 0, b"bar".to_vec());
    db.write_batch(batch).unwrap();

    // Before expiry
    assert!(db.get(b"foo").unwrap().is_some());

    // Move to expiry boundary
    db.set_time(5_005);
    assert!(db.get(b"foo").unwrap().is_none());

    cleanup_test_root(root);
}
