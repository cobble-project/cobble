use super::*;
use crate::config::VolumeDescriptor;
use crate::db_state::MultiLSMTreeVersion;
use crate::file::{FileManager, FileSystemRegistry};
use crate::lsm::LSMTreeVersion;
use crate::metrics_manager::MetricsManager;
use crate::schema::SchemaManager;
use crate::{Config, Db, WriteBatch};
use serial_test::serial;
use std::collections::VecDeque;

fn cleanup_root(path: &str) {
    let _ = std::fs::remove_dir_all(path);
}

#[test]
#[serial(file)]
fn test_db_iterator_uses_projected_family_schema_width() {
    let root = "/tmp/db_iterator_projected_family_schema_width";
    let _ = std::fs::remove_dir_all(root);
    let registry = FileSystemRegistry::new();
    let fs = registry
        .get_or_register(format!("file://{}", root))
        .expect("register file fs");
    let metrics_manager = Arc::new(MetricsManager::new("db-iterator-test"));
    let file_manager = Arc::new(
        FileManager::with_defaults(Arc::clone(&fs), Arc::clone(&metrics_manager))
            .expect("file manager"),
    );
    let vlog_store = Arc::new(VlogStore::new(file_manager, 4096, usize::MAX));

    let schema_manager = Arc::new(SchemaManager::new(2));
    let mut builder = schema_manager.builder();
    builder
        .add_column(0, None, None, Some("metrics".to_string()))
        .unwrap();
    builder
        .add_column(1, None, None, Some("metrics".to_string()))
        .unwrap();
    let schema = builder.commit();
    let projected_schema = schema.project_in_family(1, &[1]);

    let snapshot = Arc::new(DbState {
        seq_id: 0,
        topology_epoch: 0,
        bucket_ranges: Vec::new(),
        multi_lsm_version: MultiLSMTreeVersion::new(LSMTreeVersion { levels: Vec::new() }),
        vlog_version: crate::vlog::VlogVersion::new(),
        active: None,
        active_schema: None,
        min_source_schema_by_cf: Vec::new(),
        immutables: VecDeque::new(),
        truncation_cursors: crate::db_state::new_truncation_cursors(),
        suggested_base_snapshot_id: None,
    });

    let iter = DbIterator::new(
        Vec::new(),
        Vec::new(),
        DbIteratorOptions {
            end_bound: None,
            lower_bound_exclusive: None,
            max_rows: None,
            snapshot,
            memtable_manager: None,
            access_guard: None,
            vlog_store,
            ttl_provider: Arc::new(TTLProvider::disabled()),
            schema: projected_schema,
            schema_aware: false,
            schema_manager,
            selected_columns: None,
            column_family_id: 1,
            should_stop_at_block_boundary: false,
        },
    );

    assert_eq!(iter.num_columns, 1);
    let _ = std::fs::remove_dir_all(root);
}

#[test]
#[serial(file)]
fn test_db_iterator_consume_next_row_passes_bytes_key() {
    let root = "/tmp/db_iterator_consume_next_row";
    cleanup_root(root);

    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}/db", root)),
        num_columns: 2,
        total_buckets: 4,
        ..Config::default()
    };
    let db = Db::open(config, vec![0u16..=3u16]).unwrap();
    let mut batch = WriteBatch::new();
    batch.put(0, b"key1", 0, b"a0");
    batch.put(0, b"key1", 1, b"a1");
    batch.put(0, b"key2", 0, b"b0");
    db.write_batch(batch).unwrap();

    let mut iter = db.scan(0, b"key1"..b"key9").unwrap();
    let mut rows = Vec::new();
    while let Some(row) = iter
        .consume_next_row(|key, columns| Ok((key.clone(), columns.to_vec())))
        .unwrap()
    {
        rows.push(row);
    }

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].0.as_ref(), b"key1");
    assert_eq!(rows[0].1.len(), 2);
    assert_eq!(rows[0].1[0].as_deref(), Some(b"a0".as_slice()));
    assert_eq!(rows[0].1[1].as_deref(), Some(b"a1".as_slice()));
    assert_eq!(rows[1].0.as_ref(), b"key2");
    assert_eq!(rows[1].1[0].as_deref(), Some(b"b0".as_slice()));
    assert_eq!(rows[1].1[1].as_deref(), None);

    cleanup_root(root);
}
