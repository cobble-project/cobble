use super::*;
use crate::list::{ListConfig, ListRetainMode};
use bytes::Bytes;
use cobble::VolumeDescriptor;
use std::thread;
use std::time::Duration;
use uuid::Uuid;

fn apply_test_schema(db: &mut StructuredSingleDb) {
    db.update_schema()
        .add_list_column(
            None,
            1,
            ListConfig {
                max_elements: Some(3),
                retain_mode: ListRetainMode::Last,
                preserve_element_ttl: false,
            },
        )
        .commit()
        .unwrap();
}

fn test_config(root: &str) -> Config {
    Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        num_columns: 1,
        total_buckets: 2,
        snapshot_on_flush: true,
        ..Config::default()
    }
}

#[test]
fn test_structured_single_db_put_get_scan() {
    let root = format!("/tmp/ds_single_put_get_{}", Uuid::new_v4());
    let mut db = StructuredSingleDb::open(test_config(&root)).unwrap();
    apply_test_schema(&mut db);

    db.put(0, b"k1", 0, Bytes::from_static(b"v0")).unwrap();
    db.merge(0, b"k1", 1, vec![Bytes::from_static(b"a")])
        .unwrap();
    db.merge(0, b"k1", 1, vec![Bytes::from_static(b"b")])
        .unwrap();

    let row = db.get(0, b"k1").unwrap().expect("row exists");
    assert_eq!(
        row[0],
        Some(StructuredColumnValue::Bytes(Bytes::from_static(b"v0")))
    );
    assert_eq!(
        row[1],
        Some(StructuredColumnValue::List(vec![
            Bytes::from_static(b"a"),
            Bytes::from_static(b"b"),
        ]))
    );

    let mut iter = db.scan(0, b"k0".as_ref()..b"k9".as_ref()).unwrap();
    let first = iter.next().expect("one row").unwrap();
    assert_eq!(first.0.as_ref(), b"k1");
    assert!(iter.next().is_none());
    drop(iter);

    db.close().unwrap();
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn test_structured_single_db_write_batch() {
    let root = format!("/tmp/ds_single_batch_{}", Uuid::new_v4());
    let mut db = StructuredSingleDb::open(test_config(&root)).unwrap();
    apply_test_schema(&mut db);

    let mut batch = db.new_write_batch();
    batch.put(0, b"k1", 0, Bytes::from_static(b"v0")).unwrap();
    batch
        .merge(0, b"k1", 1, vec![Bytes::from_static(b"a")])
        .unwrap();
    batch
        .merge(0, b"k1", 1, vec![Bytes::from_static(b"b")])
        .unwrap();
    db.write_batch(batch).unwrap();

    let row = db.get(0, b"k1").unwrap().expect("row exists");
    assert_eq!(
        row[1],
        Some(StructuredColumnValue::List(vec![
            Bytes::from_static(b"a"),
            Bytes::from_static(b"b"),
        ]))
    );

    db.close().unwrap();
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn test_structured_single_db_delete() {
    let root = format!("/tmp/ds_single_delete_{}", Uuid::new_v4());
    let mut db = StructuredSingleDb::open(test_config(&root)).unwrap();
    apply_test_schema(&mut db);

    db.put(0, b"k1", 0, Bytes::from_static(b"v0")).unwrap();
    assert!(db.get(0, b"k1").unwrap().is_some());
    db.delete(0, b"k1", 0).unwrap();
    // After deleting column 0, the row may still be present but column 0 is None
    let row = db.get(0, b"k1").unwrap();
    if let Some(row) = row {
        assert_eq!(row[0], None);
    }

    db.close().unwrap();
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn test_structured_single_db_snapshot_lifecycle() {
    let root = format!("/tmp/ds_single_snap_{}", Uuid::new_v4());
    let mut db = StructuredSingleDb::open(test_config(&root)).unwrap();
    apply_test_schema(&mut db);

    db.put(0, b"k1", 0, Bytes::from_static(b"v0")).unwrap();
    let snap_id = db.snapshot().unwrap();
    // Snapshot ID is allocated from 0, just check it succeeds
    thread::sleep(Duration::from_millis(300));

    let snapshots = db.list_snapshots().unwrap();
    assert!(!snapshots.is_empty());
    assert_eq!(snapshots[0].id, snap_id);

    db.close().unwrap();
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn test_structured_single_db_get_with_projection_reindexes_schema() {
    let root = format!("/tmp/ds_single_get_projection_{}", Uuid::new_v4());
    let mut db = StructuredSingleDb::open(test_config(&root)).unwrap();
    apply_test_schema(&mut db);
    db.put(0, b"k1", 0, Bytes::from_static(b"v0")).unwrap();
    db.merge(0, b"k1", 1, vec![Bytes::from_static(b"a")])
        .unwrap();
    db.merge(0, b"k1", 1, vec![Bytes::from_static(b"b")])
        .unwrap();

    let row = db
        .get_with_options(0, b"k1", &StructuredReadOptions::for_column(1))
        .unwrap()
        .expect("row exists");
    assert_eq!(row.len(), 1);
    assert_eq!(
        row[0],
        Some(StructuredColumnValue::List(vec![
            Bytes::from_static(b"a"),
            Bytes::from_static(b"b"),
        ]))
    );

    db.close().unwrap();
    let _ = std::fs::remove_dir_all(root);
}
