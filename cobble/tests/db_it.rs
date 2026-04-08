use std::collections::HashMap;

use cobble::paths::bucket_snapshot_manifest_path;
use cobble::test_utils::read_metadata_payload_from_path_for_test;
use cobble::{
    CompactionPolicyKind, Config, Db, MemtableType, MetricValue, ReadOptions, Reader, ReaderConfig,
    ScanOptions, SingleDb, TimeProviderKind, U64CounterMergeOperator, VolumeDescriptor,
    VolumeUsageKind, WriteBatch,
};
use serde_json::Value as JsonValue;
use size::Size;
use std::path::Path;
use std::sync::Arc;

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
        if let Ok(payload) = read_metadata_payload_from_path_for_test(&full_path)
            && let Ok(contents) = std::str::from_utf8(&payload)
        {
            return contents.to_string();
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    let payload = read_metadata_payload_from_path_for_test(full_path).expect("read manifest");
    std::str::from_utf8(&payload)
        .expect("manifest utf8")
        .to_string()
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

fn schema_file_path(root: &str, db_id: &str, schema_id: u64) -> String {
    format!("{}/{}/schema/schema-{}", root, db_id, schema_id)
}

fn active_snapshot_segments_from_manifest(
    manifest_json: &JsonValue,
) -> Option<Vec<(String, String, String, u64, u64, Option<u64>, u64, u64, u64)>> {
    let segments = manifest_json.get("active_memtable_data")?.as_array()?;
    let mut out = Vec::with_capacity(segments.len());
    for segment in segments {
        let path = segment.get("path")?.as_str()?.to_string();
        let memtable_type = segment.get("memtable_type")?.as_str()?.to_string();
        let memtable_id = segment.get("memtable_id")?.as_str()?.to_string();
        let start_offset = segment.get("start_offset")?.as_u64()?;
        let end_offset = segment.get("end_offset")?.as_u64()?;
        out.push((
            path,
            memtable_type,
            memtable_id,
            start_offset,
            end_offset,
            segment.get("vlog_file_seq").and_then(|v| v.as_u64()),
            segment
                .get("vlog_start_offset")
                .and_then(|v| v.as_u64())
                .unwrap_or(0),
            segment
                .get("vlog_end_offset")
                .and_then(|v| v.as_u64())
                .unwrap_or(0),
            segment
                .get("vlog_data_file_offset")
                .and_then(|v| v.as_u64())
                .unwrap_or(0),
        ));
    }
    Some(out)
}

fn snapshot_tree_file_paths(manifest_json: &JsonValue) -> Vec<String> {
    let mut paths = Vec::new();
    if let Some(tree_levels) = manifest_json
        .get("tree_levels")
        .and_then(|value| value.as_array())
    {
        for levels in tree_levels {
            for level in levels.as_array().into_iter().flatten() {
                for file in level
                    .get("files")
                    .and_then(|value| value.as_array())
                    .into_iter()
                    .flatten()
                {
                    if let Some(path) = file.get("path").and_then(|value| value.as_str()) {
                        paths.push(path.to_string());
                    }
                }
            }
        }
    }
    if let Some(tree_edits) = manifest_json
        .get("tree_level_edits")
        .and_then(|value| value.as_array())
    {
        for tree_edit in tree_edits {
            for level_edit in tree_edit
                .get("level_edits")
                .and_then(|value| value.as_array())
                .into_iter()
                .flatten()
            {
                for file in level_edit
                    .get("new_files")
                    .and_then(|value| value.as_array())
                    .into_iter()
                    .flatten()
                {
                    if let Some(path) = file.get("path").and_then(|value| value.as_str()) {
                        paths.push(path.to_string());
                    }
                }
            }
        }
    }
    paths
}

fn snapshot_vlog_file_paths(manifest_json: &JsonValue) -> Vec<String> {
    manifest_json
        .get("vlog_files")
        .and_then(|value| value.as_array())
        .into_iter()
        .flatten()
        .filter_map(|entry| entry.get("path").and_then(|value| value.as_str()))
        .map(str::to_string)
        .collect()
}

fn config_with_data_file_type(config: Config, data_file_type: &str) -> Config {
    let mut json = serde_json::to_value(config).expect("serialize config");
    json["data_file_type"] = JsonValue::String(data_file_type.to_string());
    serde_json::from_value(json).expect("deserialize config with data_file_type")
}

fn snapshot_tree_file_types(manifest_json: &JsonValue) -> Vec<String> {
    let mut kinds = Vec::new();
    if let Some(tree_levels) = manifest_json
        .get("tree_levels")
        .and_then(|value| value.as_array())
    {
        for levels in tree_levels {
            for level in levels.as_array().into_iter().flatten() {
                for file in level
                    .get("files")
                    .and_then(|value| value.as_array())
                    .into_iter()
                    .flatten()
                {
                    if let Some(file_type) = file.get("file_type").and_then(|value| value.as_str())
                    {
                        kinds.push(file_type.to_string());
                    }
                }
            }
        }
    }
    if let Some(tree_edits) = manifest_json
        .get("tree_level_edits")
        .and_then(|value| value.as_array())
    {
        for tree_edit in tree_edits {
            for level_edit in tree_edit
                .get("level_edits")
                .and_then(|value| value.as_array())
                .into_iter()
                .flatten()
            {
                for file in level_edit
                    .get("new_files")
                    .and_then(|value| value.as_array())
                    .into_iter()
                    .flatten()
                {
                    if let Some(file_type) = file.get("file_type").and_then(|value| value.as_str())
                    {
                        kinds.push(file_type.to_string());
                    }
                }
            }
        }
    }
    kinds
}

fn open_db(config: Config) -> Db {
    let total_buckets = config.total_buckets;
    let full_range = 0u16..=u16::try_from(total_buckets - 1).expect("total_buckets must fit u16");
    Db::open(config, std::iter::once(full_range).collect()).unwrap()
}

#[test]
#[serial_test::serial(file)]
fn test_db_put_get_large_dataset() {
    let root = "/tmp/db_it_large_put_get";
    cleanup_test_root(root);
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        memtable_capacity: Size::from_mib(4),
        memtable_buffer_count: 2,
        num_columns: 1,
        l0_file_limit: 2,
        write_stall_limit: None,
        l1_base_bytes: Size::from_mib(16),
        level_size_multiplier: 2,
        max_level: 4,
        compaction_policy: CompactionPolicyKind::RoundRobin,
        block_cache_size: Size::from_const(0),
        base_file_size: Size::from_mib(4),
        log_path: None,
        log_console: true,
        log_level: log::LevelFilter::Debug,
        sst_bloom_filter_enabled: true,
        ..Config::default()
    };
    let db = open_db(config);

    let target_bytes = 256 * 1024 * 1024;
    let value_len = 64 * 1024;
    let mut total = 0usize;
    let mut expected: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
    let mut idx = 0u32;

    while total < target_bytes {
        let key = format!("user:{:08}", idx).into_bytes();
        let value = vec![b'v'; value_len];
        db.put(0, &key, 0, value.clone())
            .expect("Put should succeed");
        expected.insert(key, value);
        total += value_len;
        idx += 1;
    }

    for (key, expected_value) in expected.iter() {
        let value = db.get(0, key).unwrap().expect("Value present");
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
        memtable_capacity: Size::from_mib(4),
        memtable_buffer_count: 2,
        num_columns: 1,
        l0_file_limit: 2,
        write_stall_limit: None,
        l1_base_bytes: Size::from_mib(16),
        level_size_multiplier: 2,
        max_level: 4,
        compaction_policy: CompactionPolicyKind::RoundRobin,
        block_cache_size: Size::from_const(0),
        base_file_size: Size::from_mib(4),
        log_path: None,
        log_console: true,
        log_level: log::LevelFilter::Debug,
        sst_bloom_filter_enabled: true,
        ..Config::default()
    };
    let db = open_db(config);

    let target_bytes = 256 * 1024 * 1024;
    let value_len = 64 * 1024;
    let mut total = 0usize;
    let mut expected: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
    let mut idx = 0u32;

    while total < target_bytes {
        let key = format!("user:{:08}", idx).into_bytes();
        let value = vec![b'v'; value_len];
        let mut batch = WriteBatch::new();
        batch.put(0, &key, 0, value.clone());
        db.write_batch(batch).unwrap();
        expected.insert(key, value);
        total += value_len;
        idx += 1;
    }

    for (key, expected_value) in expected.iter() {
        let value = db.get(0, key).unwrap().expect("value present");
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
        memtable_capacity: Size::from_mib(4),
        memtable_buffer_count: 2,
        num_columns: 1,
        l0_file_limit: 2,
        write_stall_limit: None,
        l1_base_bytes: Size::from_mib(16),
        level_size_multiplier: 2,
        max_level: 4,
        compaction_policy: CompactionPolicyKind::RoundRobin,
        block_cache_size: Size::from_const(0),
        base_file_size: Size::from_mib(4),
        value_separation_threshold: Some(Size::from_kib(1)),
        log_path: None,
        log_console: true,
        log_level: log::LevelFilter::Debug,
        sst_bloom_filter_enabled: true,
        ..Config::default()
    };
    let db = open_db(config);

    let target_bytes = 256 * 1024 * 1024;
    let value_len = 8 * 1024;
    let mut total = 0usize;
    let mut expected: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
    let mut idx = 0u32;

    while total < target_bytes {
        let key = format!("user:{:08}", idx).into_bytes();
        let value = vec![b's'; value_len];
        db.put(0, &key, 0, value.clone())
            .expect("Put should succeed");
        expected.insert(key, value);
        total += value_len;
        idx += 1;
    }

    for (key, expected_value) in expected.iter() {
        let value = db.get(0, key).unwrap().expect("Value present");
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
        memtable_capacity: Size::from_mib(4),
        memtable_buffer_count: 2,
        num_columns: 1,
        l0_file_limit: 2,
        write_stall_limit: None,
        l1_base_bytes: Size::from_mib(16),
        level_size_multiplier: 2,
        max_level: 4,
        compaction_policy: CompactionPolicyKind::RoundRobin,
        block_cache_size: Size::from_const(0),
        base_file_size: Size::from_mib(4),
        value_separation_threshold: Some(Size::from_kib(1)),
        log_path: None,
        log_console: true,
        log_level: log::LevelFilter::Debug,
        sst_bloom_filter_enabled: true,
        ..Config::default()
    };
    let db = open_db(config);

    let target_bytes = 256 * 1024 * 1024;
    let value_len = 8 * 1024;
    let mut total = 0usize;
    let mut expected: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
    let mut idx = 0u32;

    while total < target_bytes {
        let key = format!("user:{:08}", idx).into_bytes();
        let value = vec![b's'; value_len];
        let mut batch = WriteBatch::new();
        batch.put(0, &key, 0, value.clone());
        db.write_batch(batch).unwrap();
        expected.insert(key, value);
        total += value_len;
        idx += 1;
    }

    for (key, expected_value) in expected.iter() {
        let value = db.get(0, key).unwrap().expect("value present");
        let col = value[0].as_ref().unwrap();
        assert_eq!(col.as_ref(), expected_value.as_slice());
    }

    cleanup_test_root(root);
}

#[test]
#[serial_test::serial(file)]
fn test_db_counter_merge_large_dataset_with_compaction_and_file_read() {
    let root = "/tmp/db_it_counter_merge_compaction";
    cleanup_test_root(root);
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        memtable_capacity: Size::from_kib(8),
        memtable_buffer_count: 2,
        num_columns: 1,
        l0_file_limit: 2,
        write_stall_limit: None,
        l1_base_bytes: Size::from_kib(64),
        level_size_multiplier: 2,
        max_level: 4,
        compaction_policy: CompactionPolicyKind::RoundRobin,
        block_cache_size: Size::from_const(0),
        base_file_size: Size::from_kib(32),
        sst_bloom_filter_enabled: true,
        ..Config::default()
    };
    let db = open_db(config);
    let mut schema = db.update_schema();
    schema
        .set_column_operator(0, Arc::new(U64CounterMergeOperator))
        .unwrap();
    let _ = schema.commit();

    let key_count = 2_000u32;
    let mut expected = HashMap::new();
    for i in 0..key_count {
        let key = format!("counter:{:05}", i).into_bytes();
        let base = (i % 7 + 1) as u64;
        db.put(0, &key, 0, base.to_le_bytes()).unwrap();
        let mut sum = base;
        for delta in 1..=4u64 {
            db.merge(0, &key, 0, delta.to_le_bytes()).unwrap();
            sum += delta;
        }
        expected.insert(key, sum);
    }

    let db_id = db.id().to_string();

    let mut compaction_happened = false;
    for _ in 0..200 {
        let metrics = db.metrics();
        compaction_happened = metrics.iter().any(|sample| {
            sample.name == "compaction_write_bytes_total"
                && sample
                    .labels
                    .iter()
                    .any(|(key, value)| key == "db_id" && value == &db_id)
                && matches!(sample.value, MetricValue::Counter(v) if v > 0)
        });
        if compaction_happened {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    assert!(
        compaction_happened,
        "expected compaction_write_bytes_total > 0"
    );

    for i in 0..200u32 {
        let key = format!("counter:{:05}", i).into_bytes();
        db.merge(0, &key, 0, 10u64.to_le_bytes()).unwrap();
        *expected.get_mut(&key).unwrap() += 10;
    }

    for (key, expected_sum) in expected {
        let value = db.get(0, &key).unwrap().expect("value present");
        let bytes = value[0].as_ref().unwrap();
        let actual = u64::from_le_bytes(bytes.as_ref().try_into().unwrap());
        assert_eq!(actual, expected_sum);
    }
    db.close().unwrap();

    cleanup_test_root(root);
}

#[test]
#[serial_test::serial(file)]
fn test_schema_persisted_and_restored_across_open_modes() {
    let root = "/tmp/db_schema_persistence";
    cleanup_test_root(root);
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        memtable_capacity: Size::from_kib(8),
        memtable_buffer_count: 2,
        num_columns: 1,
        block_cache_size: Size::from_const(0),
        sst_bloom_filter_enabled: true,
        ..Config::default()
    };
    let db = open_db(config.clone());
    let mut schema = db.update_schema();
    schema
        .set_column_operator(0, Arc::new(U64CounterMergeOperator))
        .unwrap();
    let _ = schema.commit();

    db.put(0, b"counter", 0, 3u64.to_le_bytes()).unwrap();
    db.merge(0, b"counter", 0, 4u64.to_le_bytes()).unwrap();

    let snapshot_id = db.snapshot().unwrap();
    let manifest = wait_for_manifest_in_db(root, db.id(), snapshot_id);
    let manifest_json: JsonValue = serde_json::from_str(&manifest).unwrap();
    let schema_id = manifest_json
        .get("latest_schema_id")
        .and_then(|value| value.as_u64())
        .expect("manifest latest_schema_id");
    let schema_path = format!("{}/{}/schema/schema-{}", root, db.id(), schema_id);
    assert!(Path::new(&schema_path).exists(), "missing schema file");
    db.close().unwrap();

    let db_id = db.id().to_string();
    let expected = 7u64;
    let read_counter = |value: Option<Vec<Option<bytes::Bytes>>>| -> u64 {
        let value = value.expect("value present");
        let bytes = value[0].as_ref().expect("column value");
        u64::from_le_bytes(bytes.as_ref().try_into().unwrap())
    };

    let read_only = Db::open_read_only(config.clone(), snapshot_id, db_id.clone()).unwrap();
    assert_eq!(
        read_counter(read_only.get(0, b"counter").unwrap()),
        expected
    );

    let from_snapshot = Db::open_from_snapshot(config.clone(), snapshot_id, db_id.clone()).unwrap();
    assert_eq!(
        read_counter(from_snapshot.get(0, b"counter").unwrap()),
        expected
    );
    from_snapshot.close().unwrap();

    let resumed = Db::resume(config, db_id).unwrap();
    assert_eq!(read_counter(resumed.get(0, b"counter").unwrap()), expected);
    resumed.close().unwrap();
    cleanup_test_root(root);
}

#[test]
#[serial_test::serial(file)]
fn test_db_get_filters_columns() {
    let root = "/tmp/db_it_get_filter_columns";
    cleanup_test_root(root);
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        memtable_capacity: Size::from_kib(4),
        memtable_buffer_count: 2,
        num_columns: 3,
        block_cache_size: Size::from_const(0),
        sst_bloom_filter_enabled: true,
        ..Config::default()
    };
    let db = open_db(config);

    for i in 0..1000 {
        let key = format!("key{:04}", i).into_bytes();
        db.put(0, &key, 0, format!("v1:{:04}", i).into_bytes())
            .expect("Put should succeed");
        db.put(0, &key, 1, format!("v2:{:04}", i).into_bytes())
            .expect("Put should succeed");
        db.put(0, &key, 2, format!("v3:{:04}", i).into_bytes())
            .expect("Put should succeed");
    }

    let options = ReadOptions::for_columns(vec![2, 0]);
    for i in 0..1000 {
        let key = format!("key{:04}", i).into_bytes();
        let value = db
            .get_with_options(0, &key, &options)
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
        memtable_capacity: Size::from_kib(4),
        memtable_buffer_count: 2,
        num_columns: 1,
        block_cache_size: Size::from_const(0),
        sst_bloom_filter_enabled: true,
        ..Config::default()
    };
    let db = open_db(config);
    let payload = vec![b'd'; 64];

    db.put(0, b"key", 0, payload.clone()).unwrap();
    for i in 0..200 {
        let key = format!("fill{:02}", i);
        db.put(0, key.as_bytes(), 0, payload.clone()).unwrap();
    }
    db.delete(0, b"key", 0).unwrap();
    for i in 200..400 {
        let key = format!("fill{:03}", i);
        db.put(0, key.as_bytes(), 0, payload.clone()).unwrap();
    }

    assert!(db.get(0, b"key").unwrap().is_none());
    cleanup_test_root(root);
}

#[test]
#[serial_test::serial(file)]
fn test_db_merge_with_large_values() {
    let root = "/tmp/db_it_merge_large_values";
    cleanup_test_root(root);
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        memtable_capacity: Size::from_kib(4),
        memtable_buffer_count: 2,
        num_columns: 1,
        block_cache_size: Size::from_const(0),
        sst_bloom_filter_enabled: true,
        ..Config::default()
    };
    let db = open_db(config);
    let base = vec![b'a'; 64];
    let suffix = vec![b'b'; 32];

    db.put(0, b"key", 0, base.clone()).unwrap();
    for i in 0..200 {
        let key = format!("fill{:02}", i);
        db.put(0, key.as_bytes(), 0, base.clone()).unwrap();
    }
    db.merge(0, b"key", 0, suffix.clone()).unwrap();
    for i in 200..400 {
        let key = format!("fill{:03}", i);
        db.put(0, key.as_bytes(), 0, base.clone()).unwrap();
    }

    let value = db.get(0, b"key").unwrap().expect("value present");
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
        memtable_capacity: Size::from_mib(4),
        memtable_buffer_count: 2,
        num_columns: 1,
        l0_file_limit: 2,
        write_stall_limit: None,
        l1_base_bytes: Size::from_mib(16),
        level_size_multiplier: 2,
        max_level: 4,
        compaction_policy: CompactionPolicyKind::RoundRobin,
        block_cache_size: Size::from_const(0),
        base_file_size: Size::from_mib(4),
        ttl_enabled: true,
        default_ttl_seconds: None,
        time_provider: TimeProviderKind::Manual,
        log_path: None,
        log_console: false,
        log_level: log::LevelFilter::Info,
        sst_bloom_filter_enabled: true,
        ..Config::default()
    };
    let db = open_db(config);

    // Put with explicit TTL of 10 seconds
    let mut batch = WriteBatch::new();
    batch.put_with_options(
        0,
        b"key",
        0,
        b"value".to_vec(),
        &cobble::WriteOptions::with_ttl(10),
    );
    db.write_batch(batch).unwrap();

    // At t=1000, value should be visible
    let v = db.get(0, b"key").unwrap().unwrap();
    assert_eq!(v[0].as_ref().unwrap().as_ref(), b"value");

    // Advance time past expiry
    db.set_time(1_011);
    assert!(db.get(0, b"key").unwrap().is_none());

    cleanup_test_root(root);
}

#[test]
#[serial_test::serial(file)]
fn test_db_ttl_default_ttl_with_manual_time() {
    let root = "/tmp/db_it_ttl_default";
    cleanup_test_root(root);

    let config = Config {
        memtable_capacity: Size::from_mib(4),
        memtable_buffer_count: 2,
        num_columns: 1,
        l0_file_limit: 2,
        write_stall_limit: None,
        l1_base_bytes: Size::from_mib(16),
        level_size_multiplier: 2,
        max_level: 4,
        compaction_policy: CompactionPolicyKind::RoundRobin,
        block_cache_size: Size::from_const(0),
        base_file_size: Size::from_mib(4),
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
    let db = open_db(config);

    // Put without explicit TTL uses default (5s)
    let mut batch = WriteBatch::new();
    batch.put(0, b"foo", 0, b"bar".to_vec());
    db.write_batch(batch).unwrap();

    // Before expiry
    assert!(db.get(0, b"foo").unwrap().is_some());

    // Move to expiry boundary
    db.set_time(5_005);
    assert!(db.get(0, b"foo").unwrap().is_none());

    cleanup_test_root(root);
}

#[test]
#[serial_test::serial(file)]
fn test_db_snapshot_creates_manifest() {
    let root = "/tmp/db_snapshot_manifest";
    cleanup_test_root(root);
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        memtable_capacity: Size::from_const(128),
        memtable_buffer_count: 2,
        num_columns: 1,
        total_buckets: 8,
        block_cache_size: Size::from_const(0),
        sst_bloom_filter_enabled: true,
        ..Config::default()
    };
    let db = open_db(config);

    let mut batch = WriteBatch::new();
    batch.put(0, b"k1", 0, b"v1".to_vec());
    db.write_batch(batch).unwrap();

    let snapshot_id = db.snapshot().unwrap();
    let manifest = wait_for_manifest_in_db(root, db.id(), snapshot_id);
    assert!(manifest.contains("\"id\":"));
    assert!(manifest.contains("\"tree_levels\""));
    assert!(manifest.contains("\"lsm_tree_bucket_ranges\""));
    assert!(manifest.contains("\"path\":\""));

    cleanup_test_root(root);
}

#[test]
#[serial_test::serial(file)]
fn test_single_db_snapshot_updates_global_manifest() {
    let root = "/tmp/single_db_snapshot";
    cleanup_test_root(root);
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        memtable_capacity: Size::from_const(128),
        memtable_buffer_count: 2,
        num_columns: 1,
        total_buckets: 8,
        block_cache_size: Size::from_const(0),
        sst_bloom_filter_enabled: true,
        ..Config::default()
    };
    let db = SingleDb::open(config.clone()).unwrap();

    db.put(0, b"k1", 0, b"v1".to_vec()).unwrap();
    db.put(0, b"k2", 0, b"v2".to_vec()).unwrap();
    db.put(0, b"x1", 0, b"vx".to_vec()).unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    let global_id = db
        .snapshot_with_callback(move |result| {
            let _ = tx.send(result);
        })
        .unwrap();
    let manifest = rx
        .recv()
        .expect("snapshot callback dropped")
        .expect("snapshot materialized");
    assert_eq!(global_id, manifest.id);

    let mut proxy = Reader::open_current(ReaderConfig::from_config(&config)).unwrap();
    let value = proxy.get(0, b"k1").unwrap().expect("value present");
    let col = value[0].as_ref().unwrap();
    assert_eq!(col.as_ref(), b"v1");
    let mut iter = proxy.scan(0, b"k1".as_slice()..b"kz".as_slice()).unwrap();
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
fn test_single_db_close_waits_snapshot_global_materialization() {
    let root = "/tmp/single_db_close_waits_snapshot";
    cleanup_test_root(root);
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        memtable_capacity: Size::from_const(128),
        memtable_buffer_count: 2,
        num_columns: 1,
        total_buckets: 8,
        block_cache_size: Size::from_const(0),
        sst_bloom_filter_enabled: true,
        ..Config::default()
    };
    let db = SingleDb::open(config.clone()).unwrap();
    db.put(0, b"k1", 0, b"v1".to_vec()).unwrap();
    let _global_snapshot_id = db.snapshot().unwrap();
    db.close().unwrap();

    let mut proxy = Reader::open_current(ReaderConfig::from_config(&config)).unwrap();
    let value = proxy.get(0, b"k1").unwrap().expect("value present");
    assert_eq!(value[0].as_ref().unwrap().as_ref(), b"v1");
    cleanup_test_root(root);
}

#[test]
#[serial_test::serial(file)]
fn test_db_snapshot_read_only_get_with_vec_memtable() {
    let root = "/tmp/db_snapshot_readonly_vec_memtable";
    cleanup_test_root(root);
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        memtable_capacity: Size::from_const(256),
        memtable_buffer_count: 2,
        memtable_type: MemtableType::Vec,
        num_columns: 1,
        block_cache_size: Size::from_const(0),
        sst_bloom_filter_enabled: true,
        ..Config::default()
    };
    let db = open_db(config.clone());
    let mut expected = HashMap::new();
    for i in 0..128u32 {
        let key = format!("k{:04}", i).into_bytes();
        let value = format!("value-{:04}", i).into_bytes();
        db.put(0, &key, 0, value.clone()).unwrap();
        expected.insert(key, value);
    }

    let snapshot_id = db.snapshot().unwrap();
    let _ = wait_for_manifest_in_db(root, db.id(), snapshot_id);
    db.close().unwrap();

    let ro = Db::open_read_only(config, snapshot_id, db.id()).unwrap();
    for (key, expected_value) in expected.iter() {
        let value = ro.get(0, key).unwrap().expect("value present");
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
        memtable_capacity: Size::from_const(256),
        memtable_buffer_count: 2,
        memtable_type: MemtableType::Vec,
        num_columns: 1,
        value_separation_threshold: Some(Size::from_const(8)),
        block_cache_size: Size::from_const(0),
        sst_bloom_filter_enabled: true,
        ..Config::default()
    };
    let db = open_db(config.clone());
    let mut expected = HashMap::new();
    for i in 0..64u32 {
        let key = format!("k{:04}", i).into_bytes();
        let value = vec![b'a' + (i % 26) as u8; 96];
        db.put(0, &key, 0, value.clone()).unwrap();
        expected.insert(key, value);
    }

    let snapshot_id = db.snapshot().unwrap();
    let _ = wait_for_manifest_in_db(root, db.id(), snapshot_id);
    db.close().unwrap();

    let ro = Db::open_read_only(config, snapshot_id, db.id()).unwrap();
    for (key, expected_value) in expected.iter() {
        let value = ro.get(0, key).unwrap().expect("value present");
        let col = value[0].as_ref().unwrap();
        assert_eq!(col.as_ref(), expected_value.as_slice());
    }
    cleanup_test_root(root);
}

#[test]
#[serial_test::serial(file)]
fn test_db_snapshot_read_only_get_with_skiplist_memtable() {
    let root = "/tmp/db_snapshot_readonly_skiplist_memtable";
    cleanup_test_root(root);
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        memtable_capacity: Size::from_const(256),
        memtable_buffer_count: 2,
        memtable_type: MemtableType::Skiplist,
        num_columns: 1,
        block_cache_size: Size::from_const(0),
        sst_bloom_filter_enabled: true,
        ..Config::default()
    };
    let db = open_db(config.clone());
    let mut expected = HashMap::new();
    for i in 0..128u32 {
        let key = format!("k{:04}", i).into_bytes();
        let value = format!("value-{:04}", i).into_bytes();
        db.put(0, &key, 0, value.clone()).unwrap();
        expected.insert(key, value);
    }

    let snapshot_id = db.snapshot().unwrap();
    let _ = wait_for_manifest_in_db(root, db.id(), snapshot_id);
    db.close().unwrap();

    let ro = Db::open_read_only(config, snapshot_id, db.id()).unwrap();
    for (key, expected_value) in expected.iter() {
        let value = ro.get(0, key).unwrap().expect("value present");
        let col = value[0].as_ref().unwrap();
        assert_eq!(col.as_ref(), expected_value.as_slice());
    }
    cleanup_test_root(root);
}

#[test]
#[serial_test::serial(file)]
fn test_db_snapshot_read_only_get_with_skiplist_memtable_separated_values() {
    let root = "/tmp/db_snapshot_readonly_skiplist_memtable_separated";
    cleanup_test_root(root);
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        memtable_capacity: Size::from_const(256),
        memtable_buffer_count: 2,
        memtable_type: MemtableType::Skiplist,
        num_columns: 1,
        value_separation_threshold: Some(Size::from_const(8)),
        block_cache_size: Size::from_const(0),
        sst_bloom_filter_enabled: true,
        ..Config::default()
    };
    let db = open_db(config.clone());
    let mut expected = HashMap::new();
    for i in 0..64u32 {
        let key = format!("k{:04}", i).into_bytes();
        let value = vec![b'a' + (i % 26) as u8; 96];
        db.put(0, &key, 0, value.clone()).unwrap();
        expected.insert(key, value);
    }

    let snapshot_id = db.snapshot().unwrap();
    let _ = wait_for_manifest_in_db(root, db.id(), snapshot_id);
    db.close().unwrap();

    let ro = Db::open_read_only(config, snapshot_id, db.id()).unwrap();
    for (key, expected_value) in expected.iter() {
        let value = ro.get(0, key).unwrap().expect("value present");
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
        memtable_capacity: Size::from_const(128),
        memtable_buffer_count: 2,
        num_columns: 1,
        block_cache_size: Size::from_const(0),
        sst_bloom_filter_enabled: true,
        ..Config::default()
    };
    let db = open_db(config.clone());

    let mut batch = WriteBatch::new();
    batch.put(0, b"k1", 0, b"v1".to_vec());
    db.write_batch(batch).unwrap();

    let snapshot_id = db.snapshot().unwrap();
    let _ = wait_for_manifest_in_db(root, db.id(), snapshot_id);
    db.close().unwrap();

    let ro = Db::open_read_only(config, snapshot_id, db.id()).unwrap();
    let value = ro.get(0, b"k1").unwrap().expect("value present");
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
        memtable_capacity: Size::from_const(128),
        memtable_buffer_count: 2,
        num_columns: 1,
        value_separation_threshold: Some(Size::from_const(8)),
        block_cache_size: Size::from_const(0),
        sst_bloom_filter_enabled: true,
        ..Config::default()
    };
    let db = open_db(config.clone());
    let large = b"value-larger-than-threshold";

    let mut batch = WriteBatch::new();
    batch.put(0, b"k1", 0, large.to_vec());
    db.write_batch(batch).unwrap();

    let snapshot_id = db.snapshot().unwrap();
    let _ = wait_for_manifest_in_db(root, db.id(), snapshot_id);
    db.close().unwrap();

    let ro = Db::open_read_only(config, snapshot_id, db.id()).unwrap();
    let value = ro.get(0, b"k1").unwrap().expect("value present");
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
        memtable_capacity: Size::from_const(128),
        memtable_buffer_count: 2,
        num_columns: 1,
        value_separation_threshold: Some(Size::from_const(8)),
        block_cache_size: Size::from_const(0),
        sst_bloom_filter_enabled: true,
        ..Config::default()
    };
    let db = open_db(config.clone());
    let huge = vec![b'h'; 128];
    db.put(0, b"aa", 0, b"ignore-left").unwrap();
    db.put(0, b"k1", 0, b"v1").unwrap();
    db.put(0, b"k2", 0, huge.clone()).unwrap();
    db.put(0, b"z1", 0, b"ignore-right").unwrap();

    let snapshot_id = db.snapshot().unwrap();
    let _ = wait_for_manifest_in_db(root, db.id(), snapshot_id);
    db.close().unwrap();

    let ro = Db::open_read_only(config, snapshot_id, db.id()).unwrap();
    let mut iter = ro.scan(0, b"k1".as_slice()..b"kz".as_slice()).unwrap();
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
        memtable_capacity: Size::from_const(128),
        memtable_buffer_count: 2,
        num_columns: 1,
        block_cache_size: Size::from_const(0),
        sst_bloom_filter_enabled: true,
        ..Config::default()
    };
    let db = open_db(config.clone());
    db.put(0, b"k1", 0, b"v1").unwrap();
    let base_snapshot_id = db.snapshot().unwrap();
    let _ = wait_for_manifest_in_db(root, db.id(), base_snapshot_id);

    db.put(0, b"k2", 0, b"v2").unwrap();
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
    let value1 = ro.get(0, b"k1").unwrap().expect("k1 value present");
    assert_eq!(value1[0].as_ref().unwrap().as_ref(), b"v1");
    let value2 = ro.get(0, b"k2").unwrap().expect("k2 value present");
    assert_eq!(value2[0].as_ref().unwrap().as_ref(), b"v2");

    let writable = Db::open_from_snapshot(config, incremental_snapshot_id, db.id()).unwrap();
    let value1 = writable.get(0, b"k1").unwrap().expect("k1 value present");
    assert_eq!(value1[0].as_ref().unwrap().as_ref(), b"v1");
    let value2 = writable.get(0, b"k2").unwrap().expect("k2 value present");
    assert_eq!(value2[0].as_ref().unwrap().as_ref(), b"v2");

    cleanup_test_root(root);
}

#[test]
#[serial_test::serial(file)]
fn test_db_snapshot_volume_uploads_files_and_keeps_incremental_manifest() {
    let root = "/tmp/db_snapshot_volume_upload";
    let primary_root = format!("{}/primary", root);
    let snapshot_root = format!("{}/snapshot", root);
    cleanup_test_root(root);
    let snapshot_volume_prefix = format!("file://{}", snapshot_root);
    let config = Config {
        volumes: vec![
            VolumeDescriptor::new(
                format!("file://{}", primary_root),
                vec![VolumeUsageKind::PrimaryDataPriorityHigh],
            ),
            VolumeDescriptor::new(
                format!("file://{}", snapshot_root),
                vec![VolumeUsageKind::Snapshot, VolumeUsageKind::Meta],
            ),
        ],
        memtable_capacity: Size::from_const(128),
        memtable_buffer_count: 2,
        num_columns: 1,
        block_cache_size: Size::from_const(0),
        value_separation_threshold: Some(Size::from_const(16)),
        sst_bloom_filter_enabled: true,
        ..Config::default()
    };
    let db = open_db(config);
    db.put(0, b"k1", 0, vec![b'a'; 128]).unwrap();
    let first_snapshot_id = db.snapshot().unwrap();
    let first_manifest = wait_for_manifest_in_db(&snapshot_root, db.id(), first_snapshot_id);
    let first_manifest_json: JsonValue = serde_json::from_str(&first_manifest).unwrap();
    let first_tree_paths = snapshot_tree_file_paths(&first_manifest_json);
    assert!(!first_tree_paths.is_empty());
    assert!(
        first_tree_paths
            .iter()
            .all(|path| path.starts_with(&snapshot_volume_prefix))
    );
    let first_vlog_paths = snapshot_vlog_file_paths(&first_manifest_json);
    assert!(!first_vlog_paths.is_empty());
    assert!(
        first_vlog_paths
            .iter()
            .all(|path| path.starts_with(&snapshot_volume_prefix))
    );

    db.put(0, b"k2", 0, vec![b'b'; 128]).unwrap();
    let second_snapshot_id = db.snapshot().unwrap();
    let second_manifest = wait_for_manifest_in_db(&snapshot_root, db.id(), second_snapshot_id);
    let second_manifest_json: JsonValue = serde_json::from_str(&second_manifest).unwrap();
    assert_eq!(
        second_manifest_json
            .get("base_snapshot_id")
            .and_then(|value| value.as_u64()),
        Some(first_snapshot_id)
    );
    let second_tree_paths = snapshot_tree_file_paths(&second_manifest_json);
    assert!(!second_tree_paths.is_empty());
    assert!(
        second_tree_paths
            .iter()
            .all(|path| path.starts_with(&snapshot_volume_prefix))
    );
    let second_vlog_paths = snapshot_vlog_file_paths(&second_manifest_json);
    assert!(!second_vlog_paths.is_empty());
    assert!(
        second_vlog_paths
            .iter()
            .all(|path| path.starts_with(&snapshot_volume_prefix))
    );
    assert!(Path::new(&format!("{}/{}/data", primary_root, db.id())).exists());
    db.close().unwrap();
    cleanup_test_root(root);
}

#[test]
#[serial_test::serial(file)]
fn test_db_restore_from_readonly_snapshot_volume_migrates_files_and_disables_first_incremental() {
    let root = "/tmp/db_restore_readonly_snapshot_volume";
    let old_root = format!("{}/old", root);
    let new_primary_root = format!("{}/new-primary", root);
    let new_snapshot_root = format!("{}/new-snapshot", root);
    cleanup_test_root(root);

    let source_config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", old_root)),
        memtable_capacity: Size::from_const(64),
        memtable_buffer_count: 2,
        num_columns: 1,
        block_cache_size: Size::from_const(0),
        sst_bloom_filter_enabled: true,
        ..Config::default()
    };
    let source_db = open_db(source_config);
    for idx in 0..64u16 {
        let key = format!("k{:04}", idx);
        source_db.put(0, key.as_bytes(), 0, b"value").unwrap();
    }
    let snapshot_id = source_db.snapshot().unwrap();
    let source_manifest = wait_for_manifest_in_db(&old_root, source_db.id(), snapshot_id);
    let source_manifest_json: JsonValue = serde_json::from_str(&source_manifest).unwrap();
    assert!(
        !snapshot_tree_file_paths(&source_manifest_json).is_empty(),
        "expected source snapshot to contain tree files"
    );
    let db_id = source_db.id().to_string();
    source_db.close().unwrap();

    let manifest_rel_path = bucket_snapshot_manifest_path(&db_id, snapshot_id);
    let old_manifest_path = format!("{}/{}", old_root, manifest_rel_path);
    let new_manifest_path = format!("{}/{}", new_snapshot_root, manifest_rel_path);
    if let Some(parent) = Path::new(&new_manifest_path).parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::copy(&old_manifest_path, &new_manifest_path).unwrap();
    let old_schema_dir = format!("{}/{}/schema", old_root, db_id);
    let new_schema_dir = format!("{}/{}/schema", new_snapshot_root, db_id);
    std::fs::create_dir_all(&new_schema_dir).unwrap();
    for entry in std::fs::read_dir(&old_schema_dir).unwrap() {
        let entry = entry.unwrap();
        if entry.file_type().unwrap().is_file() {
            std::fs::copy(
                entry.path(),
                format!("{}/{}", new_schema_dir, entry.file_name().to_string_lossy()),
            )
            .unwrap();
        }
    }

    let restore_config = Config {
        volumes: vec![
            VolumeDescriptor::new(
                format!("file://{}", new_primary_root),
                vec![VolumeUsageKind::PrimaryDataPriorityHigh],
            ),
            VolumeDescriptor::new(
                format!("file://{}", new_snapshot_root),
                vec![VolumeUsageKind::Snapshot, VolumeUsageKind::Meta],
            ),
            VolumeDescriptor::new(
                format!("file://{}", old_root),
                vec![VolumeUsageKind::Readonly],
            ),
        ],
        memtable_capacity: Size::from_const(64),
        memtable_buffer_count: 2,
        num_columns: 1,
        block_cache_size: Size::from_const(0),
        sst_bloom_filter_enabled: true,
        ..Config::default()
    };
    let restored = Db::open_from_snapshot(restore_config, snapshot_id, db_id.clone()).unwrap();
    let restored_value = restored.get(0, b"k0001").unwrap().expect("restored value");
    assert_eq!(restored_value[0].as_ref().unwrap().as_ref(), b"value");
    let new_primary_data_dir = format!("{}/{}/data", new_primary_root, db_id);
    assert!(Path::new(&new_primary_data_dir).exists());
    assert!(
        std::fs::read_dir(&new_primary_data_dir)
            .unwrap()
            .next()
            .is_some(),
        "expected restored files to be migrated into new primary volume"
    );

    let first_snapshot_after_restore = restored.snapshot().unwrap();
    let restore_manifest = wait_for_manifest_in_db(
        &new_snapshot_root,
        restored.id(),
        first_snapshot_after_restore,
    );
    let restore_manifest_json: JsonValue = serde_json::from_str(&restore_manifest).unwrap();
    assert!(
        restore_manifest_json.get("base_snapshot_id").is_none(),
        "first snapshot after restore should not use incremental base"
    );
    restored.close().unwrap();
    cleanup_test_root(root);
}

#[test]
#[serial_test::serial(file)]
fn test_db_snapshot_multi_volume_selection_reuse_and_expire_regression() {
    let root = "/tmp/db_snapshot_multi_volume_regression";
    let primary_root = format!("{}/primary", root);
    let snapshot_a_root = format!("{}/snapshot-a", root);
    let snapshot_b_root = format!("{}/snapshot-b", root);
    cleanup_test_root(root);
    let snapshot_a_prefix = format!("file://{}", snapshot_a_root);
    let config = Config {
        volumes: vec![
            VolumeDescriptor::new(
                format!("file://{}", primary_root),
                vec![VolumeUsageKind::PrimaryDataPriorityHigh],
            ),
            VolumeDescriptor::new(
                format!("file://{}", snapshot_a_root),
                vec![VolumeUsageKind::Snapshot],
            ),
            VolumeDescriptor::new(
                format!("file://{}", snapshot_b_root),
                vec![VolumeUsageKind::Snapshot],
            ),
        ],
        memtable_capacity: Size::from_const(128),
        memtable_buffer_count: 2,
        num_columns: 1,
        block_cache_size: Size::from_const(0),
        value_separation_threshold: Some(Size::from_const(16)),
        sst_bloom_filter_enabled: true,
        ..Config::default()
    };
    let db = open_db(config);

    db.put(0, b"k1", 0, vec![b'a'; 128]).unwrap();
    let first_snapshot_id = db.snapshot().unwrap();
    let first_manifest = wait_for_manifest_in_db(&snapshot_a_root, db.id(), first_snapshot_id);
    let first_manifest_json: JsonValue = serde_json::from_str(&first_manifest).unwrap();
    assert!(
        snapshot_tree_file_paths(&first_manifest_json)
            .iter()
            .all(|path| path.starts_with(&snapshot_a_prefix))
    );
    assert!(
        snapshot_vlog_file_paths(&first_manifest_json)
            .iter()
            .all(|path| path.starts_with(&snapshot_a_prefix))
    );

    db.put(0, b"k2", 0, vec![b'b'; 128]).unwrap();
    let second_snapshot_id = db.snapshot().unwrap();
    let second_manifest = wait_for_manifest_in_db(&snapshot_a_root, db.id(), second_snapshot_id);
    let second_manifest_json: JsonValue = serde_json::from_str(&second_manifest).unwrap();
    assert_eq!(
        second_manifest_json
            .get("base_snapshot_id")
            .and_then(|value| value.as_u64()),
        Some(first_snapshot_id)
    );
    assert!(
        snapshot_tree_file_paths(&second_manifest_json)
            .iter()
            .all(|path| path.starts_with(&snapshot_a_prefix))
    );
    assert!(
        snapshot_vlog_file_paths(&second_manifest_json)
            .iter()
            .all(|path| path.starts_with(&snapshot_a_prefix))
    );

    let first_manifest_path = format!(
        "{}/{}",
        snapshot_a_root,
        bucket_snapshot_manifest_path(db.id(), first_snapshot_id)
    );
    assert!(db.expire_snapshot(first_snapshot_id).unwrap());
    assert!(Path::new(&first_manifest_path).exists());
    assert!(db.expire_snapshot(second_snapshot_id).unwrap());
    let _ = db.expire_snapshot(first_snapshot_id).unwrap();
    wait_for_missing(&first_manifest_path);
    db.close().unwrap();
    cleanup_test_root(root);
}

#[test]
#[serial_test::serial(file)]
fn test_db_incremental_snapshot_keeps_base_manifest_live() {
    let root = "/tmp/db_snapshot_incremental_dependency";
    cleanup_test_root(root);
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        memtable_capacity: Size::from_const(128),
        memtable_buffer_count: 2,
        num_columns: 1,
        block_cache_size: Size::from_const(0),
        sst_bloom_filter_enabled: true,
        ..Config::default()
    };
    let db = open_db(config);
    db.put(0, b"k1", 0, b"v1").unwrap();
    let base_snapshot_id = db.snapshot().unwrap();
    let _ = wait_for_manifest_in_db(root, db.id(), base_snapshot_id);
    db.put(0, b"k2", 0, b"v2").unwrap();
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
        memtable_capacity: Size::from_const(128),
        memtable_buffer_count: 2,
        num_columns: 1,
        block_cache_size: Size::from_const(0),
        sst_bloom_filter_enabled: true,
        ..Config::default()
    };
    let db = open_db(config.clone());

    let mut batch = WriteBatch::new();
    batch.put(0, b"k1", 0, b"v1".to_vec());
    db.write_batch(batch).unwrap();

    let snapshot_id = db.snapshot().unwrap();
    let _ = wait_for_manifest_in_db(root, db.id(), snapshot_id);
    db.close().unwrap();

    let writable = Db::open_from_snapshot(config, snapshot_id, db.id()).unwrap();
    let mut batch = WriteBatch::new();
    batch.put(0, b"k2", 0, b"v2".to_vec());
    writable.write_batch(batch).unwrap();

    let value = writable.get(0, b"k1").unwrap().expect("value present");
    let col = value[0].as_ref().unwrap();
    assert_eq!(col.as_ref(), b"v1");

    let value = writable.get(0, b"k2").unwrap().expect("value present");
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
        memtable_capacity: Size::from_const(128),
        memtable_buffer_count: 2,
        num_columns: 1,
        total_buckets: 8,
        block_cache_size: Size::from_const(0),
        sst_bloom_filter_enabled: true,
        ..Config::default()
    };
    let db = open_db(config.clone());
    db.put(0, b"k1", 0, b"v1").unwrap();
    let snapshot_id = db.snapshot().unwrap();
    assert!(db.retain_snapshot(snapshot_id));
    let _ = wait_for_manifest_in_db(root, db.id(), snapshot_id);
    db.close().unwrap();

    let writable = Db::open_from_snapshot(config.clone(), snapshot_id, db.id()).unwrap();
    assert!(!writable.expire_snapshot(snapshot_id).unwrap());
    writable.close().unwrap();

    let resume_config = config;
    let writable = Db::resume(resume_config, db.id()).unwrap();
    let bucket_snapshot = writable.shard_snapshot_input(snapshot_id).unwrap();
    assert_eq!(bucket_snapshot.ranges, vec![0u16..=7u16]);

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
fn test_db_multi_lsm_snapshot_restore_and_resume() {
    let root = "/tmp/db_multi_lsm_snapshot_restore_resume";
    cleanup_test_root(root);
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        memtable_capacity: Size::from_const(256),
        memtable_buffer_count: 2,
        num_columns: 1,
        total_buckets: 4,
        block_cache_size: Size::from_const(0),
        sst_bloom_filter_enabled: true,
        ..Config::default()
    };
    let ranges = vec![0u16..=1u16, 2u16..=3u16];
    let db = Db::open(config.clone(), ranges.clone()).unwrap();
    db.put(0, b"k-left", 0, b"v-left").unwrap();
    db.put(3, b"k-right", 0, b"v-right").unwrap();
    let snapshot_id = db.snapshot().unwrap();
    let manifest = wait_for_manifest_in_db(root, db.id(), snapshot_id);
    let manifest_json: JsonValue = serde_json::from_str(&manifest).unwrap();
    assert_eq!(
        manifest_json
            .get("bucket_ranges")
            .and_then(|v| v.as_array())
            .map_or(0, |v| v.len()),
        2
    );
    assert_eq!(
        manifest_json
            .get("tree_levels")
            .and_then(|v| v.as_array())
            .map_or(0, |v| v.len()),
        2
    );
    assert_eq!(
        manifest_json
            .get("lsm_tree_bucket_ranges")
            .and_then(|v| v.as_array())
            .map_or(0, |v| v.len()),
        2
    );
    db.close().unwrap();

    let restored = Db::open_from_snapshot(config.clone(), snapshot_id, db.id()).unwrap();
    let left = restored.get(0, b"k-left").unwrap().expect("left value");
    assert_eq!(left[0].as_ref().unwrap().as_ref(), b"v-left");
    let right = restored.get(3, b"k-right").unwrap().expect("right value");
    assert_eq!(right[0].as_ref().unwrap().as_ref(), b"v-right");
    restored.close().unwrap();

    let resumed = Db::resume(config, db.id()).unwrap();
    let left = resumed
        .get(0, b"k-left")
        .unwrap()
        .expect("left value after resume");
    assert_eq!(left[0].as_ref().unwrap().as_ref(), b"v-left");
    let right = resumed
        .get(3, b"k-right")
        .unwrap()
        .expect("right value after resume");
    assert_eq!(right[0].as_ref().unwrap().as_ref(), b"v-right");
    let bucket_snapshot = resumed.shard_snapshot_input(snapshot_id).unwrap();
    assert_eq!(bucket_snapshot.ranges, ranges);
    resumed.close().unwrap();
    cleanup_test_root(root);
}

#[test]
#[serial_test::serial(file)]
fn test_db_parquet_snapshot_restore_and_read_only() {
    let root = "/tmp/db_parquet_snapshot_restore_readonly";
    cleanup_test_root(root);
    let config = config_with_data_file_type(
        Config {
            volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
            memtable_capacity: Size::from_const(256),
            memtable_buffer_count: 2,
            num_columns: 1,
            total_buckets: 4,
            block_cache_size: Size::from_const(0),
            ..Config::default()
        },
        "parquet",
    );
    let ranges = vec![0u16..=1u16, 2u16..=3u16];
    let db = Db::open(config.clone(), ranges.clone()).unwrap();
    db.put(0, b"k-left", 0, b"v-left").unwrap();
    db.put(3, b"k-right", 0, b"v-right").unwrap();
    let snapshot_id = db.snapshot().unwrap();
    let manifest = wait_for_manifest_in_db(root, db.id(), snapshot_id);
    let manifest_json: JsonValue = serde_json::from_str(&manifest).unwrap();
    let file_types = snapshot_tree_file_types(&manifest_json);
    assert!(
        !file_types.is_empty() && file_types.iter().all(|t| t == "parquet"),
        "expected parquet files in manifest, got {:?}",
        file_types
    );
    db.close().unwrap();

    let read_only = Db::open_read_only(config.clone(), snapshot_id, db.id()).unwrap();
    let left = read_only.get(0, b"k-left").unwrap().expect("left value");
    assert_eq!(left[0].as_ref().unwrap().as_ref(), b"v-left");
    let right = read_only.get(3, b"k-right").unwrap().expect("right value");
    assert_eq!(right[0].as_ref().unwrap().as_ref(), b"v-right");

    let restored = Db::open_from_snapshot(config.clone(), snapshot_id, db.id()).unwrap();
    let left = restored
        .get(0, b"k-left")
        .unwrap()
        .expect("left value after restore");
    assert_eq!(left[0].as_ref().unwrap().as_ref(), b"v-left");
    let right = restored
        .get(3, b"k-right")
        .unwrap()
        .expect("right value after restore");
    assert_eq!(right[0].as_ref().unwrap().as_ref(), b"v-right");
    restored.close().unwrap();
    cleanup_test_root(root);
}

#[test]
#[serial_test::serial(file)]
fn test_db_metrics_list() {
    let root = "/tmp/db_metrics_list";
    cleanup_test_root(root);
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        memtable_capacity: Size::from_const(128),
        memtable_buffer_count: 2,
        num_columns: 1,
        block_cache_size: Size::from_const(0),
        sst_bloom_filter_enabled: true,
        ..Config::default()
    };
    let db = open_db(config);

    let mut batch = WriteBatch::new();
    batch.put(0, b"k1", 0, b"v1".to_vec());
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
        memtable_capacity: Size::from_const(128),
        memtable_buffer_count: 2,
        num_columns: 1,
        block_cache_size: Size::from_const(0),
        ..Config::default()
    };
    let db = open_db(config);

    let mut batch = WriteBatch::new();
    batch.put(0, b"k1", 0, b"v1".to_vec());
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
fn test_db_expire_snapshot_releases_schema_files() {
    let root = "/tmp/db_snapshot_expire_schema_gc";
    cleanup_test_root(root);
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        memtable_capacity: Size::from_const(128),
        memtable_buffer_count: 2,
        num_columns: 1,
        block_cache_size: Size::from_const(0),
        sst_bloom_filter_enabled: true,
        ..Config::default()
    };
    let db = open_db(config);
    let mut schema = db.update_schema();
    schema.add_column(1, None, None).unwrap();
    let _ = schema.commit();

    db.put(0, b"k1", 1, b"v1").unwrap();
    let snapshot_id = db.snapshot().unwrap();
    let manifest = wait_for_manifest_in_db(root, db.id(), snapshot_id);
    let manifest_json: JsonValue = serde_json::from_str(&manifest).unwrap();
    let latest_schema_id = manifest_json
        .get("latest_schema_id")
        .and_then(|value| value.as_u64())
        .expect("manifest latest_schema_id");
    let latest_schema_path = schema_file_path(root, db.id(), latest_schema_id);
    assert!(
        Path::new(&latest_schema_path).exists(),
        "missing latest schema file {}",
        latest_schema_path
    );
    let schema0_path = schema_file_path(root, db.id(), 0);
    let schema0_exists = Path::new(&schema0_path).exists();

    let manifest_path = format!(
        "{}/{}",
        root,
        bucket_snapshot_manifest_path(db.id(), snapshot_id)
    );
    assert!(db.expire_snapshot(snapshot_id).unwrap());
    wait_for_missing(&manifest_path);
    wait_for_missing(&latest_schema_path);
    if schema0_exists {
        wait_for_missing(&schema0_path);
    }

    cleanup_test_root(root);
}

#[test]
#[serial_test::serial(file)]
fn test_db_snapshot_uses_active_memtable_incremental_data_when_under_threshold() {
    let root = "/tmp/db_snapshot_active_incremental";
    cleanup_test_root(root);
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        memtable_capacity: Size::from_kib(1),
        memtable_buffer_count: 2,
        num_columns: 1,
        active_memtable_incremental_snapshot_ratio: 0.9,
        block_cache_size: Size::from_const(0),
        sst_bloom_filter_enabled: true,
        ..Config::default()
    };
    let db = open_db(config);
    db.put(0, b"k1", 0, b"v1").unwrap();

    let first_snapshot = db.snapshot().unwrap();
    let first_manifest = wait_for_manifest_in_db(root, db.id(), first_snapshot);
    let first_manifest_json: JsonValue = serde_json::from_str(&first_manifest).unwrap();
    let first_segments = active_snapshot_segments_from_manifest(&first_manifest_json)
        .expect("missing active memtable snapshot data");
    assert_eq!(first_segments.len(), 1);
    let (first_path, first_type, first_memtable_id, first_start, first_end, _, _, _, _) =
        &first_segments[0];
    assert!(!first_type.is_empty());
    assert_eq!(*first_start, 0);
    assert!(first_end > first_start);
    let first_file = format!("{}/{}/{}", root, db.id(), first_path);
    let first_data = std::fs::read(&first_file).unwrap();
    assert_eq!(first_data.len() as u64, *first_end - *first_start);

    db.put(0, b"k2", 0, b"v2").unwrap();
    let second_snapshot = db.snapshot().unwrap();
    let second_manifest = wait_for_manifest_in_db(root, db.id(), second_snapshot);
    let second_manifest_json: JsonValue = serde_json::from_str(&second_manifest).unwrap();
    let second_segments = active_snapshot_segments_from_manifest(&second_manifest_json)
        .expect("missing second active memtable snapshot data");
    assert_eq!(second_segments.len(), 2);
    let (
        _,
        second_first_type,
        second_first_memtable_id,
        second_first_start,
        second_first_end,
        _,
        _,
        _,
        _,
    ) = &second_segments[0];
    assert_eq!(*second_first_type, *first_type);
    assert_eq!(second_first_memtable_id, first_memtable_id);
    assert_eq!(*second_first_start, *first_start);
    assert_eq!(*second_first_end, *first_end);
    let (second_path, second_type, second_memtable_id, second_start, second_end, _, _, _, _) =
        &second_segments[1];
    assert_eq!(*second_type, *first_type);
    assert_eq!(second_memtable_id, first_memtable_id);
    assert_eq!(*second_start, *first_end);
    assert!(second_end > second_start);
    let second_file = format!("{}/{}/{}", root, db.id(), second_path);
    let second_data = std::fs::read(&second_file).unwrap();
    assert_eq!(second_data.len() as u64, *second_end - *second_start);

    cleanup_test_root(root);
}

#[test]
#[serial_test::serial(file)]
fn test_db_snapshot_active_incremental_flushes_vlog_entries() {
    let root = "/tmp/db_snapshot_active_incremental_vlog";
    cleanup_test_root(root);
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        memtable_capacity: Size::from_kib(2),
        memtable_buffer_count: 2,
        num_columns: 1,
        value_separation_threshold: Some(Size::from_const(8)),
        active_memtable_incremental_snapshot_ratio: 0.9,
        block_cache_size: Size::from_const(0),
        sst_bloom_filter_enabled: true,
        ..Config::default()
    };
    let db = open_db(config.clone());
    let v1 = vec![b'a'; 128];
    db.put(0, b"k1", 0, v1.clone()).unwrap();

    let first_snapshot = db.snapshot().unwrap();
    let first_manifest = wait_for_manifest_in_db(root, db.id(), first_snapshot);
    let first_manifest_json: JsonValue = serde_json::from_str(&first_manifest).unwrap();
    assert_eq!(
        first_manifest_json
            .get("vlog_files")
            .and_then(|v| v.as_array())
            .map_or(0, |v| v.len()),
        0
    );
    let first_segments = active_snapshot_segments_from_manifest(&first_manifest_json)
        .expect("missing active_memtable_data");
    assert_eq!(first_segments.len(), 1);
    let (
        first_path,
        first_type,
        first_memtable_id,
        first_data_start,
        first_data_end,
        first_vlog_file_seq,
        first_vlog_start,
        first_vlog_end,
        first_vlog_file_offset,
    ) = &first_segments[0];
    assert!(!first_type.is_empty());
    assert_eq!(*first_data_start, 0);
    assert!(first_data_end > first_data_start);
    assert!(first_vlog_end > first_vlog_start);
    assert!(first_vlog_file_seq.is_some());
    assert_eq!(*first_vlog_file_offset, first_data_end - first_data_start);
    let first_file = format!("{}/{}/{}", root, db.id(), first_path);
    let first_data = std::fs::read(first_file).unwrap();
    let expected_first_size =
        (first_data_end - first_data_start) + (first_vlog_end - first_vlog_start);
    assert_eq!(first_data.len() as u64, expected_first_size);
    let value1 = db.get(0, b"k1").unwrap().expect("k1 value");
    assert_eq!(value1[0].as_ref().unwrap().as_ref(), v1.as_slice());

    let v2 = vec![b'b'; 128];
    db.put(0, b"k2", 0, v2.clone()).unwrap();
    let second_snapshot = db.snapshot().unwrap();
    let second_manifest = wait_for_manifest_in_db(root, db.id(), second_snapshot);
    let second_manifest_json: JsonValue = serde_json::from_str(&second_manifest).unwrap();
    assert_eq!(
        second_manifest_json
            .get("vlog_files")
            .and_then(|v| v.as_array())
            .map_or(0, |v| v.len()),
        0
    );
    let second_segments = active_snapshot_segments_from_manifest(&second_manifest_json)
        .expect("missing second active_memtable_data");
    assert_eq!(second_segments.len(), 2);
    let (
        _,
        second_first_type,
        second_first_memtable_id,
        second_first_data_start,
        second_first_data_end,
        second_first_vlog_file_seq,
        second_first_vlog_start,
        second_first_vlog_end,
        second_first_vlog_file_offset,
    ) = &second_segments[0];
    assert_eq!(*second_first_type, *first_type);
    assert_eq!(second_first_memtable_id, first_memtable_id);
    assert_eq!(*second_first_data_start, *first_data_start);
    assert_eq!(*second_first_data_end, *first_data_end);
    assert_eq!(*second_first_vlog_file_seq, *first_vlog_file_seq);
    assert_eq!(*second_first_vlog_start, *first_vlog_start);
    assert_eq!(*second_first_vlog_end, *first_vlog_end);
    assert_eq!(*second_first_vlog_file_offset, *first_vlog_file_offset);
    let (
        second_path,
        second_type,
        second_memtable_id,
        second_data_start,
        second_data_end,
        second_vlog_file_seq,
        second_vlog_start,
        second_vlog_end,
        second_vlog_file_offset,
    ) = &second_segments[1];
    assert_eq!(*second_type, *first_type);
    assert_eq!(second_memtable_id, first_memtable_id);
    assert_eq!(*second_data_start, *first_data_end);
    assert!(second_data_end > second_data_start);
    assert_eq!(*second_vlog_file_seq, *first_vlog_file_seq);
    assert_eq!(*second_vlog_start, *first_vlog_end);
    assert!(second_vlog_end > second_vlog_start);
    assert_eq!(
        *second_vlog_file_offset,
        second_data_end - second_data_start
    );
    let second_file = format!("{}/{}/{}", root, db.id(), second_path);
    let second_data = std::fs::read(second_file).unwrap();
    let expected_second_size =
        (second_data_end - second_data_start) + (second_vlog_end - second_vlog_start);
    assert_eq!(second_data.len() as u64, expected_second_size);
    let value2 = db.get(0, b"k2").unwrap().expect("k2 value");
    assert_eq!(value2[0].as_ref().unwrap().as_ref(), v2.as_slice());

    let db_id = db.id().to_string();
    db.close().unwrap();

    let from_snapshot = Db::open_from_snapshot(config.clone(), second_snapshot, db_id.clone())
        .expect("open from snapshot with active memtable replay");
    let restored_v1 = from_snapshot.get(0, b"k1").unwrap().expect("restored k1");
    assert_eq!(restored_v1[0].as_ref().unwrap().as_ref(), v1.as_slice());
    let restored_v2 = from_snapshot.get(0, b"k2").unwrap().expect("restored k2");
    assert_eq!(restored_v2[0].as_ref().unwrap().as_ref(), v2.as_slice());
    from_snapshot.close().unwrap();

    let resumed = Db::resume(config, db_id).expect("resume with active memtable replay");
    let resumed_v1 = resumed.get(0, b"k1").unwrap().expect("resumed k1");
    assert_eq!(resumed_v1[0].as_ref().unwrap().as_ref(), v1.as_slice());
    let resumed_v2 = resumed.get(0, b"k2").unwrap().expect("resumed k2");
    assert_eq!(resumed_v2[0].as_ref().unwrap().as_ref(), v2.as_slice());
    resumed.close().unwrap();

    cleanup_test_root(root);
}

#[test]
#[serial_test::serial(file)]
fn test_db_resume_can_continue_writes_after_active_memtable_snapshot() {
    let root = "/tmp/db_resume_after_active_memtable_snapshot";
    cleanup_test_root(root);
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        memtable_capacity: Size::from_kib(1),
        memtable_buffer_count: 2,
        num_columns: 1,
        active_memtable_incremental_snapshot_ratio: 0.9,
        block_cache_size: Size::from_const(0),
        sst_bloom_filter_enabled: true,
        ..Config::default()
    };
    let db = open_db(config.clone());
    db.put(0, b"k1", 0, b"v1").unwrap();
    db.put(0, b"k2", 0, b"v2").unwrap();

    let snapshot_id = db.snapshot().unwrap();
    let manifest = wait_for_manifest_in_db(root, db.id(), snapshot_id);
    let manifest_json: JsonValue = serde_json::from_str(&manifest).unwrap();
    let active_segments =
        active_snapshot_segments_from_manifest(&manifest_json).expect("active memtable snapshot");
    assert!(!active_segments.is_empty());
    let db_id = db.id().to_string();
    db.close().unwrap();

    let resumed = Db::resume(config.clone(), db_id.clone()).expect("resume db");
    resumed.put(0, b"k3", 0, b"v3").unwrap();
    resumed.put(0, b"k4", 0, b"v4").unwrap();
    for (key, expected) in [
        (b"k1".as_slice(), b"v1".as_slice()),
        (b"k2".as_slice(), b"v2".as_slice()),
        (b"k3".as_slice(), b"v3".as_slice()),
        (b"k4".as_slice(), b"v4".as_slice()),
    ] {
        let value = resumed
            .get(0, key)
            .unwrap()
            .expect("value present after resume");
        assert_eq!(value[0].as_ref().unwrap().as_ref(), expected);
    }
    resumed.close().unwrap();

    cleanup_test_root(root);
}

#[test]
#[serial_test::serial(file)]
fn test_db_snapshot_falls_back_to_sst_when_active_memtable_above_threshold() {
    let root = "/tmp/db_snapshot_active_incremental_fallback";
    cleanup_test_root(root);
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        memtable_capacity: Size::from_kib(1),
        memtable_buffer_count: 2,
        num_columns: 1,
        active_memtable_incremental_snapshot_ratio: 0.05,
        block_cache_size: Size::from_const(0),
        sst_bloom_filter_enabled: true,
        ..Config::default()
    };
    let db = open_db(config);
    for idx in 0..16u8 {
        let key = format!("k{}", idx);
        db.put(0, key.as_bytes(), 0, b"v").unwrap();
    }

    let snapshot_id = db.snapshot().unwrap();
    let manifest = wait_for_manifest_in_db(root, db.id(), snapshot_id);
    let manifest_json: JsonValue = serde_json::from_str(&manifest).unwrap();
    assert_eq!(
        active_snapshot_segments_from_manifest(&manifest_json).unwrap_or_default(),
        Vec::new()
    );
    let level_files: usize = manifest_json
        .get("tree_levels")
        .and_then(|v| v.as_array())
        .map_or(0, |trees| {
            trees
                .iter()
                .map(|levels| {
                    levels.as_array().map_or(0, |levels| {
                        levels
                            .iter()
                            .map(|level| {
                                level
                                    .get("files")
                                    .and_then(|files| files.as_array())
                                    .map_or(0, |files| files.len())
                            })
                            .sum::<usize>()
                    })
                })
                .sum()
        });
    assert!(level_files > 0);

    cleanup_test_root(root);
}

#[test]
#[serial_test::serial(file)]
fn test_db_snapshot_auto_expire() {
    let root = "/tmp/db_snapshot_auto_expire";
    cleanup_test_root(root);
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        memtable_capacity: Size::from_const(128),
        memtable_buffer_count: 2,
        num_columns: 1,
        block_cache_size: Size::from_const(0),
        snapshot_retention: Some(1),
        snapshot_on_flush: true,
        sst_bloom_filter_enabled: true,
        ..Config::default()
    };
    let db = open_db(config);

    let mut batch = WriteBatch::new();
    batch.put(0, b"k1", 0, b"v1".to_vec());
    db.write_batch(batch).unwrap();

    let first_id = db.snapshot().unwrap();
    let _ = wait_for_manifest_in_db(root, db.id(), first_id);

    let mut batch = WriteBatch::new();
    batch.put(0, b"k2", 0, b"v2".to_vec());
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
        memtable_capacity: Size::from_const(128),
        memtable_buffer_count: 2,
        num_columns: 1,
        block_cache_size: Size::from_const(0),
        snapshot_retention: Some(1),
        snapshot_on_flush: true,
        sst_bloom_filter_enabled: true,
        ..Config::default()
    };
    let db = open_db(config);

    let mut batch = WriteBatch::new();
    batch.put(0, b"k1", 0, b"v1".to_vec());
    db.write_batch(batch).unwrap();

    let first_id = db.snapshot().unwrap();
    let _ = wait_for_manifest_in_db(root, db.id(), first_id);
    assert!(db.retain_snapshot(first_id));

    let mut batch = WriteBatch::new();
    batch.put(0, b"k2", 0, b"v2".to_vec());
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
        memtable_capacity: Size::from_kib(1),
        memtable_buffer_count: 2,
        num_columns: 1,
        value_separation_threshold: Some(Size::from_const(64)),
        block_cache_size: Size::from_const(0),
        sst_bloom_filter_enabled: true,
        ..Config::default()
    };
    let db = open_db(config);

    let huge_sst = vec![b's'; 512];
    let huge_mem = vec![b'm'; 768];

    db.put(0, b"aaa:outside", 0, b"ignore-left").unwrap();
    db.put(0, b"mix:key", 0, b"old").unwrap();
    db.put(0, b"mix:huge-sst", 0, huge_sst.clone()).unwrap();
    for i in 0..200u32 {
        let key = format!("fill:{:04}", i).into_bytes();
        db.put(0, &key, 0, vec![b'f'; 96]).unwrap();
    }
    db.put(0, b"mix:key", 0, b"new").unwrap();
    db.put(0, b"mix:huge-mem", 0, huge_mem.clone()).unwrap();
    db.put(0, b"mix:tail", 0, b"tail").unwrap();
    db.put(0, b"zzz:outside", 0, b"ignore-right").unwrap();

    let mut iter = db
        .scan(0, b"fill:0000".as_slice()..b"mix;".as_slice())
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
        memtable_capacity: Size::from_kib(1),
        memtable_buffer_count: 2,
        num_columns: 1,
        block_cache_size: Size::from_const(0),
        sst_bloom_filter_enabled: true,
        ..Config::default()
    };
    let db = open_db(config);

    for i in 0..160u32 {
        let key = format!("r:{:04}", i).into_bytes();
        db.put(0, &key, 0, format!("v:{:04}", i).into_bytes())
            .unwrap();
    }
    db.put(0, b"q:outside", 0, b"left").unwrap();
    db.put(0, b"r:0050", 0, b"latest").unwrap();
    db.put(0, b"s:outside", 0, b"right").unwrap();

    let mut iter = db
        .scan(0, b"r:0000".as_slice()..b"r:0100".as_slice())
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

#[test]
#[serial_test::serial(file)]
fn test_db_scan_with_column_indices_flush_and_read_only_sst() {
    run_scan_with_column_indices_flush_and_read_only_case(
        "/tmp/db_it_scan_column_indices_flush_ro_sst",
        "sst",
    );
}

#[test]
#[serial_test::serial(file)]
fn test_db_scan_with_column_indices_flush_and_read_only_parquet() {
    run_scan_with_column_indices_flush_and_read_only_case(
        "/tmp/db_it_scan_column_indices_flush_ro_parquet",
        "parquet",
    );
}

fn run_scan_with_column_indices_flush_and_read_only_case(root: &str, data_file_type: &str) {
    cleanup_test_root(root);
    let config = config_with_data_file_type(
        Config {
            volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
            memtable_capacity: Size::from_kib(1),
            memtable_buffer_count: 2,
            num_columns: 3,
            block_cache_size: Size::from_const(0),
            sst_bloom_filter_enabled: true,
            ..Config::default()
        },
        data_file_type,
    );
    let db = open_db(config.clone());

    for i in 0..256u32 {
        let key = format!("proj:{:04}", i).into_bytes();
        db.put(0, &key, 0, format!("c0-{i:04}").into_bytes())
            .unwrap();
        db.put(0, &key, 1, format!("c1-{i:04}").into_bytes())
            .unwrap();
        db.put(0, &key, 2, format!("c2-{i:04}").into_bytes())
            .unwrap();
    }
    db.put(0, b"proj:0042", 0, b"c0-updated".to_vec()).unwrap();
    db.put(0, b"proj:0042", 2, b"c2-updated".to_vec()).unwrap();

    let snapshot_id = db.snapshot().unwrap();
    let manifest = wait_for_manifest_in_db(root, db.id(), snapshot_id);
    let manifest_json: JsonValue = serde_json::from_str(&manifest).unwrap();
    let file_types = snapshot_tree_file_types(&manifest_json);
    assert!(
        !file_types.is_empty() && file_types.iter().all(|t| t == data_file_type),
        "expected {data_file_type} files in manifest, got {:?}",
        file_types
    );

    let mut scan_options = ScanOptions::for_columns(vec![2, 0]);
    scan_options.read_ahead_bytes = Size::from_const(256);
    let mut iter = db
        .scan_with_options(
            0,
            b"proj:0000".as_slice()..b"proj:9999".as_slice(),
            &scan_options,
        )
        .unwrap();
    let mut seen: HashMap<Vec<u8>, (Vec<u8>, Vec<u8>)> = HashMap::new();
    while let Some(row) = iter.next() {
        let (key, columns) = row.unwrap();
        assert_eq!(columns.len(), 2);
        let col2 = columns[0]
            .as_ref()
            .expect("projected col2 present")
            .to_vec();
        let col0 = columns[1]
            .as_ref()
            .expect("projected col0 present")
            .to_vec();
        seen.insert(key.to_vec(), (col2, col0));
    }
    assert_eq!(seen.len(), 256);
    for i in 0..256u32 {
        let key = format!("proj:{:04}", i).into_bytes();
        let expected_col0 = if i == 42 {
            b"c0-updated".to_vec()
        } else {
            format!("c0-{i:04}").into_bytes()
        };
        let expected_col2 = if i == 42 {
            b"c2-updated".to_vec()
        } else {
            format!("c2-{i:04}").into_bytes()
        };
        let (actual_col2, actual_col0) = seen.get(&key).expect("row exists");
        assert_eq!(actual_col2.as_slice(), expected_col2.as_slice());
        assert_eq!(actual_col0.as_slice(), expected_col0.as_slice());
    }

    db.close().unwrap();

    let ro = Db::open_read_only(config, snapshot_id, db.id()).unwrap();
    let mut ro_iter = ro
        .scan_with_options(
            0,
            b"proj:0000".as_slice()..b"proj:9999".as_slice(),
            &ScanOptions::for_columns(vec![2, 0]),
        )
        .unwrap();
    let mut ro_seen = 0usize;
    while let Some(row) = ro_iter.next() {
        let (key, columns) = row.unwrap();
        assert_eq!(columns.len(), 2);
        let key_str = std::str::from_utf8(key.as_ref()).unwrap();
        let i: u32 = key_str["proj:".len()..].parse().unwrap();
        let expected_col0 = if i == 42 {
            b"c0-updated".to_vec()
        } else {
            format!("c0-{i:04}").into_bytes()
        };
        let expected_col2 = if i == 42 {
            b"c2-updated".to_vec()
        } else {
            format!("c2-{i:04}").into_bytes()
        };
        assert_eq!(
            columns[0]
                .as_ref()
                .expect("projected col2 present")
                .as_ref(),
            expected_col2.as_slice()
        );
        assert_eq!(
            columns[1]
                .as_ref()
                .expect("projected col0 present")
                .as_ref(),
            expected_col0.as_slice()
        );
        ro_seen += 1;
    }
    assert_eq!(ro_seen, 256);

    cleanup_test_root(root);
}

#[test]
#[serial_test::serial(file)]
fn test_db_skiplist_large_write_flush_and_scan() {
    let root = "/tmp/db_it_skiplist_large_scan";
    cleanup_test_root(root);
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        memtable_capacity: Size::from_kib(16),
        memtable_buffer_count: 2,
        memtable_type: MemtableType::Skiplist,
        num_columns: 1,
        l0_file_limit: 2,
        write_stall_limit: None,
        l1_base_bytes: Size::from_kib(64),
        level_size_multiplier: 2,
        max_level: 4,
        block_cache_size: Size::from_const(0),
        sst_bloom_filter_enabled: true,
        ..Config::default()
    };
    let db = open_db(config.clone());

    let mut expected: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
    for i in 0..2000u32 {
        let key = format!("scan:{:05}", i).into_bytes();
        let value = vec![b'a' + (i % 26) as u8; 256];
        db.put(0, &key, 0, value.clone()).unwrap();
        expected.insert(key, value);
    }
    db.put(0, b"scan:01000", 0, b"updated-1000".to_vec())
        .unwrap();
    expected.insert(b"scan:01000".to_vec(), b"updated-1000".to_vec());
    db.put(0, b"scan:01500", 0, b"updated-1500".to_vec())
        .unwrap();
    expected.insert(b"scan:01500".to_vec(), b"updated-1500".to_vec());

    let snapshot_id = db.snapshot().unwrap();
    let manifest = wait_for_manifest_in_db(root, db.id(), snapshot_id);
    let manifest_json: JsonValue = serde_json::from_str(&manifest).unwrap();
    let level_files: usize = manifest_json
        .get("tree_levels")
        .and_then(|v| v.as_array())
        .map_or(0, |trees| {
            trees
                .iter()
                .filter_map(|levels| levels.as_array())
                .flat_map(|levels| levels.iter())
                .filter_map(|level| level.get("files").and_then(|v| v.as_array()))
                .map(|files| files.len())
                .sum()
        });
    assert!(
        level_files > 0,
        "expected snapshot to include flushed files"
    );

    let mut iter = db
        .scan(0, b"scan:00000".as_slice()..b"scan:02000".as_slice())
        .unwrap();
    let mut seen: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
    while let Some(row) = iter.next() {
        let (key, columns) = row.unwrap();
        if let Some(value) = columns[0].as_ref() {
            seen.insert(key.to_vec(), value.to_vec());
        }
    }

    assert_eq!(seen.len(), expected.len());
    for (key, value) in expected {
        assert_eq!(seen.get(&key).unwrap().as_slice(), value.as_slice());
    }

    cleanup_test_root(root);
}
