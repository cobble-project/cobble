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

fn empty_snapshot() -> Arc<DbState> {
    Arc::new(DbState {
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
    })
}

#[test]
#[serial(file)]
fn test_full_projection_preserves_encoded_terminal_memtable_rows() {
    for memtable_type in [
        crate::MemtableType::Hash,
        crate::MemtableType::Skiplist,
        crate::MemtableType::Vec,
    ] {
        let root = tempfile::tempdir().unwrap();
        let db = Db::open(
            Config {
                volumes: VolumeDescriptor::single_volume(format!(
                    "file://{}",
                    root.path().display()
                )),
                num_columns: 2,
                total_buckets: 4,
                memtable_type,
                ..Config::default()
            },
            vec![0u16..=3u16],
        )
        .unwrap();
        let mut batch = WriteBatch::new();
        batch.put(0, b"key", 0, b"a");
        batch.put(0, b"key", 1, b"b");
        db.write_batch(batch).unwrap();

        let mut full = db
            .scan_with_options(
                0,
                b"key"..b"kez",
                &crate::ScanOptions::for_columns(vec![0, 1]),
            )
            .unwrap();
        assert!(
            full.inner.take_value().unwrap().unwrap().is_encoded(),
            "{memtable_type:?}: full projection should defer value decoding"
        );
        drop(full);

        let mut reordered = db
            .scan_with_options(
                0,
                b"key"..b"kez",
                &crate::ScanOptions::for_columns(vec![1, 0]),
            )
            .unwrap();
        assert!(!reordered.inner.take_value().unwrap().unwrap().is_encoded());
    }
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

    let iter = DbIterator::new(
        Vec::new(),
        Vec::new(),
        DbIteratorOptions {
            end_bound: None,
            lower_bound_exclusive: None,
            max_rows: None,
            snapshot: empty_snapshot(),
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
fn bounded_scan_stops_before_dedup_value_reads_and_lookahead() {
    use crate::iterator::SchemaTaggedIterator;
    use crate::iterator::mock_iterator::CountingMockIterator;
    use crate::sst::row_codec::{encode_key, encode_value};
    use crate::r#type::{Column, Value, ValueType};
    use std::sync::atomic::Ordering;

    let root = tempfile::tempdir().unwrap();
    let registry = FileSystemRegistry::new();
    let fs = registry
        .get_or_register(format!("file://{}", root.path().display()))
        .unwrap();
    let file_manager = Arc::new(
        FileManager::with_defaults(fs, Arc::new(MetricsManager::new("scan-upper-bound"))).unwrap(),
    );
    let vlog_store = Arc::new(VlogStore::new(file_manager, 4096, usize::MAX));
    let schema_manager = Arc::new(SchemaManager::new(1));
    let schema = schema_manager.latest_schema();
    let key = |key: &'static [u8]| encode_key(&Key::new(0, Bytes::from_static(key)));
    let value = |value: &'static [u8]| {
        encode_value(
            &Value::new(vec![Some(Column::new(
                ValueType::Put,
                Bytes::from_static(value),
            ))]),
            1,
        )
    };
    for schema_aware in [false, true] {
        for (end_key, inclusive, expected_keys, expected_next) in [
            (b"a".as_slice(), false, vec![], 0),
            (b"c".as_slice(), false, vec![b"a".as_slice()], 2),
            (
                b"c".as_slice(),
                true,
                vec![b"a".as_slice(), b"c".as_slice()],
                4,
            ),
        ] {
            let (input, counts) = CountingMockIterator::new(vec![
                (key(b"a"), value(b"a-new")),
                (key(b"a"), value(b"a-old")),
                (key(b"c"), value(b"c-new")),
                (key(b"c"), value(b"c-old")),
                // A value outside every tested range must never be decoded, even by lookahead.
                (key(b"e"), Bytes::from_static(b"corrupt-value")),
            ]);
            let mut iter = DbIterator::new(
                vec![Box::new(SchemaTaggedIterator::new(input, schema.version()))],
                Vec::new(),
                DbIteratorOptions {
                    end_bound: Some((key(end_key), inclusive)),
                    lower_bound_exclusive: None,
                    max_rows: None,
                    snapshot: empty_snapshot(),
                    memtable_manager: None,
                    access_guard: None,
                    vlog_store: Arc::clone(&vlog_store),
                    ttl_provider: Arc::new(TTLProvider::disabled()),
                    schema: Arc::clone(&schema),
                    schema_aware,
                    schema_manager: Arc::clone(&schema_manager),
                    selected_columns: None,
                    column_family_id: 0,
                    should_stop_at_block_boundary: false,
                },
            );
            iter.seek(&key(b"a")).unwrap();
            let actual = iter.by_ref().collect::<Result<Vec<_>>>().unwrap();
            assert_eq!(
                actual
                    .iter()
                    .map(|(key, _)| key.as_ref())
                    .collect::<Vec<_>>(),
                expected_keys,
                "schema_aware={schema_aware}, inclusive={inclusive}"
            );
            for (key, columns) in actual {
                let expected = if key.as_ref() == b"a" {
                    b"a-new"
                } else {
                    b"c-new"
                };
                assert_eq!(columns[0].as_deref(), Some(expected.as_slice()));
            }
            let value_keys = counts.value_keys.lock().unwrap();
            assert!(
                value_keys
                    .iter()
                    .all(|read| { expected_keys.iter().any(|expected| *read == key(expected)) })
            );
            assert_eq!(
                value_keys.len(),
                expected_keys.len() * if schema_aware { 2 } else { 1 }
            );
            drop(value_keys);
            assert_eq!(counts.next.load(Ordering::Relaxed), expected_next);
            for _ in 0..3 {
                assert!(iter.next().is_none());
            }
            assert_eq!(counts.next.load(Ordering::Relaxed), expected_next);
            iter.seek(&key(b"a")).unwrap();
            assert_eq!(iter.next().is_some(), !expected_keys.is_empty());
        }
    }
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

#[test]
fn test_owned_row_consumer_preserves_error_and_limit_semantics() {
    let root = tempfile::tempdir().unwrap();
    let db = Db::open(
        Config {
            volumes: VolumeDescriptor::single_volume(format!("file://{}", root.path().display())),
            ..Config::default()
        },
        vec![0..=0],
    )
    .unwrap();
    for key in [b"k1", b"k2", b"k3"] {
        db.put(0, key, 0, key).unwrap();
    }
    let (tx, rx) = std::sync::mpsc::channel();
    db.snapshot_with_callback(move |result| tx.send(result).unwrap())
        .unwrap();
    rx.recv_timeout(std::time::Duration::from_secs(30))
        .unwrap()
        .unwrap();

    let options = crate::ScanOptions::default().with_max_rows(1);
    for owned in [false, true] {
        let mut iter = db.scan_with_options(0, b"k0"..b"k9", &options).unwrap();
        let failure = if owned {
            iter.consume_next_row_owned::<(), _>(|_, _| {
                Err(crate::Error::InputError("consumer failed".to_string()))
            })
        } else {
            iter.consume_next_row::<(), _>(|_, _| {
                Err(crate::Error::InputError("consumer failed".to_string()))
            })
        };
        assert!(failure.is_err());
        let (key, columns) = iter
            .consume_next_row_owned(|key, columns| Ok((key.clone(), columns)))
            .unwrap()
            .unwrap();
        assert_eq!(key.as_ref(), b"k2");
        assert_eq!(columns[0].as_deref(), Some(b"k2".as_slice()));
        assert!(
            iter.consume_next_row_owned(|_, _| Ok(()))
                .unwrap()
                .is_none()
        );
        assert!(iter.next_row_with_bucket().unwrap().is_none());
    }

    let mut iter = db.scan(0, b"k0"..b"k9").unwrap();
    iter.remaining_rows = Some(0);
    assert!(
        iter.consume_next_row_owned::<(), _>(|_, _| panic!(
            "zero limit must not call the consumer"
        ))
        .unwrap()
        .is_none()
    );
}
