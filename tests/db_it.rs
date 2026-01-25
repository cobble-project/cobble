use std::collections::HashMap;

use cobble::{CompactionPolicyKind, Config, Db, WriteBatch};

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
        l1_base_bytes: 8 * 1024 * 1024,
        level_size_multiplier: 2,
        max_level: 4,
        compaction_policy: CompactionPolicyKind::RoundRobin,
        block_cache_size: 0,
        base_file_size: 1024 * 1024,
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
