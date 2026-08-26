use crate::{StructuredDb, StructuredReadOptions, StructuredSingleDb};
use bytes::Bytes;
use cobble::{Config, Error, Result, VolumeDescriptor};
use std::thread;
use std::time::Duration;
use uuid::Uuid;

use super::decode_priority_queue_row;

fn open_test_db(root: &str) -> Result<StructuredDb> {
    StructuredDb::open(
        Config {
            volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
            num_columns: 1,
            snapshot_on_flush: true,
            ..Config::default()
        },
        vec![0u16..=0u16],
    )
}

fn open_test_single_db(root: &str) -> Result<StructuredSingleDb> {
    StructuredSingleDb::open(Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        num_columns: 1,
        total_buckets: 1,
        snapshot_on_flush: true,
        ..Config::default()
    })
}

#[test]
fn test_priority_queue_offer_merge_and_poll_order() {
    let root = format!("/tmp/ds_priority_queue_order_{}", Uuid::new_v4());
    let mut db = open_test_db(&root).unwrap();
    db.new_priority_queue("jobs").unwrap();
    let queue = db.get_priority_queue("jobs").unwrap();
    assert!(
        queue
            .descriptor
            .fixed_scan_options
            .as_cobble()
            .preload_scan_cursor_block()
    );

    queue.offer(0, b"k2", b"v2").unwrap();
    queue.offer(0, b"k1", b"left").unwrap();
    queue.offer(0, b"k1", b"right").unwrap();

    let first = queue.poll(0).unwrap().expect("first poll");
    assert_eq!(first.0.as_ref(), b"k1");
    assert_eq!(first.1.as_ref(), b"leftright");

    let second = queue.poll(0).unwrap().expect("second poll");
    assert_eq!(second.0.as_ref(), b"k2");
    assert_eq!(second.1.as_ref(), b"v2");

    assert!(queue.poll(0).unwrap().is_none());
    db.close().unwrap();
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn test_priority_queue_value_preserves_bytes_allocation() {
    let key = Bytes::from_static(b"key");
    let value = Bytes::from(vec![7; 128]);
    let pointer = value.as_ptr();
    let (decoded_key, decoded_value) =
        decode_priority_queue_row((key.clone(), vec![Some(value)])).unwrap();
    assert_eq!(decoded_key, key);
    assert_eq!(decoded_value.as_ptr(), pointer);

    assert!(decode_priority_queue_row((key.clone(), Vec::new())).is_err());
    assert!(decode_priority_queue_row((key.clone(), vec![None])).is_err());
    assert!(
        decode_priority_queue_row((key, vec![Some(Bytes::from_static(b"value")), None],)).is_err()
    );
}

#[test]
fn test_priority_queue_structured_single_db_offer_peek_poll_and_cursor() {
    let root = format!("/tmp/ds_priority_queue_single_{}", Uuid::new_v4());
    let mut db = open_test_single_db(&root).unwrap();
    db.new_priority_queue("jobs").unwrap();
    let queue = db.get_priority_queue("jobs").unwrap();

    queue.offer(0, b"k2", b"v2").unwrap();
    queue.offer(0, b"k1", b"left").unwrap();
    queue.offer(0, b"k1", b"right").unwrap();

    let peeked = queue.peek(0).unwrap().expect("peek");
    assert_eq!(
        peeked,
        (Bytes::from_static(b"k1"), Bytes::from_static(b"leftright"))
    );
    assert_eq!(queue.cursor(0).unwrap(), None);

    let batch = queue.poll_batch(0, Some(2)).unwrap();
    assert_eq!(
        batch,
        vec![
            (Bytes::from_static(b"k1"), Bytes::from_static(b"leftright")),
            (Bytes::from_static(b"k2"), Bytes::from_static(b"v2")),
        ]
    );
    assert_eq!(queue.cursor(0).unwrap(), Some(b"k2".to_vec()));
    assert!(queue.poll(0).unwrap().is_none());

    drop(queue);
    db.close().unwrap();
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn test_priority_queue_delete_and_namespace_isolation() {
    let root = format!("/tmp/ds_priority_queue_delete_{}", Uuid::new_v4());
    let mut db = open_test_db(&root).unwrap();
    db.new_priority_queue("jobs-a").unwrap();
    db.new_priority_queue("jobs-b").unwrap();

    let queue_a = db.get_priority_queue("jobs-a").unwrap();
    let queue_b = db.get_priority_queue("jobs-b").unwrap();
    queue_a.offer(0, b"shared", b"va").unwrap();
    queue_b.offer(0, b"shared", b"vb").unwrap();
    queue_a.delete(0, b"missing").unwrap();
    queue_a.delete(0, b"shared").unwrap();
    assert!(queue_a.poll(0).unwrap().is_none());

    let from_b = queue_b.poll(0).unwrap().expect("poll jobs-b");
    assert_eq!(from_b.0.as_ref(), b"shared");
    assert_eq!(from_b.1.as_ref(), b"vb");

    db.close().unwrap();
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn test_priority_queue_poll_after_resume() {
    let root = format!("/tmp/ds_priority_queue_resume_{}", Uuid::new_v4());
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        num_columns: 1,
        snapshot_on_flush: true,
        ..Config::default()
    };
    let db_id = {
        let mut db = StructuredDb::open(config.clone(), vec![0u16..=0u16]).unwrap();
        let queue = db.get_or_new_priority_queue("jobs").unwrap();
        queue.offer(0, b"k2", b"v2").unwrap();
        queue.offer(0, b"k1", b"v1").unwrap();
        let _ = db.snapshot().unwrap();
        thread::sleep(Duration::from_millis(250));
        let db_id = db.id().to_string();
        db.close().unwrap();
        db_id
    };

    let resumed = StructuredDb::resume(config, db_id).unwrap();
    let queue = resumed.get_priority_queue("jobs").unwrap();
    let first = queue.poll(0).unwrap().expect("first poll");
    assert_eq!(
        first,
        (Bytes::from_static(b"k1"), Bytes::from_static(b"v1"))
    );
    let second = queue.poll(0).unwrap().expect("second poll");
    assert_eq!(
        second,
        (Bytes::from_static(b"k2"), Bytes::from_static(b"v2"))
    );
    resumed.close().unwrap();
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn test_priority_queue_poll_persists_truncation_cursor() {
    let root = format!("/tmp/ds_priority_queue_truncation_{}", Uuid::new_v4());
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        num_columns: 1,
        snapshot_on_flush: true,
        ..Config::default()
    };
    let db_id = {
        let mut db = StructuredDb::open(config.clone(), vec![0u16..=0u16]).unwrap();
        let queue = db.get_or_new_priority_queue("jobs").unwrap();
        queue.offer(0, b"k1", b"v1").unwrap();
        queue.offer(0, b"k2", b"v2").unwrap();

        let first = queue.poll(0).unwrap().expect("first poll");
        assert_eq!(
            first,
            (Bytes::from_static(b"k1"), Bytes::from_static(b"v1"))
        );
        assert_eq!(queue.cursor(0).unwrap(), Some(b"k1".to_vec()));
        let read_options = StructuredReadOptions::for_column(0).with_column_family("jobs");
        assert!(
            db.get_raw_with_options(0, b"k1", &read_options)
                .unwrap()
                .is_none()
        );
        assert!(
            db.get_raw_with_options(0, b"k2", &read_options)
                .unwrap()
                .is_some()
        );

        let _ = db.snapshot().unwrap();
        thread::sleep(Duration::from_millis(250));
        let db_id = db.id().to_string();
        db.close().unwrap();
        db_id
    };

    let resumed = StructuredDb::resume(config, db_id).unwrap();
    let queue = resumed.get_priority_queue("jobs").unwrap();
    assert_eq!(queue.cursor(0).unwrap(), Some(b"k1".to_vec()));
    let remaining = queue.poll(0).unwrap().expect("remaining poll");
    assert_eq!(
        remaining,
        (Bytes::from_static(b"k2"), Bytes::from_static(b"v2"))
    );
    assert!(queue.poll(0).unwrap().is_none());
    resumed.close().unwrap();
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn test_priority_queue_poll_batch_advances_cursor_once_to_last_key() {
    let root = format!("/tmp/ds_priority_queue_batch_{}", Uuid::new_v4());
    let mut db = open_test_db(&root).unwrap();
    db.new_priority_queue("jobs").unwrap();
    let queue = db.get_priority_queue("jobs").unwrap();
    queue.offer(0, b"k1", b"v1").unwrap();
    queue.offer(0, b"k2", b"v2").unwrap();
    queue.offer(0, b"k3", b"v3").unwrap();

    let batch = queue.poll_batch(0, Some(2)).unwrap();
    assert_eq!(
        batch,
        vec![
            (Bytes::from_static(b"k1"), Bytes::from_static(b"v1")),
            (Bytes::from_static(b"k2"), Bytes::from_static(b"v2")),
        ]
    );
    let read_options = StructuredReadOptions::for_column(0).with_column_family("jobs");
    assert!(
        db.get_raw_with_options(0, b"k1", &read_options)
            .unwrap()
            .is_none()
    );
    assert!(
        db.get_raw_with_options(0, b"k2", &read_options)
            .unwrap()
            .is_none()
    );
    assert!(
        db.get_raw_with_options(0, b"k3", &read_options)
            .unwrap()
            .is_some()
    );

    let remaining = queue.poll_batch(0, Some(10)).unwrap();
    assert_eq!(
        remaining,
        vec![(Bytes::from_static(b"k3"), Bytes::from_static(b"v3"))]
    );
    assert!(queue.poll_batch(0, Some(10)).unwrap().is_empty());
    db.close().unwrap();
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn test_priority_queue_peek_and_peek_batch_do_not_advance_cursor() {
    let root = format!("/tmp/ds_priority_queue_peek_{}", Uuid::new_v4());
    let mut db = open_test_db(&root).unwrap();
    db.new_priority_queue("jobs").unwrap();
    let queue = db.get_priority_queue("jobs").unwrap();
    queue.offer(0, b"k1", b"v1").unwrap();
    queue.offer(0, b"k2", b"v2").unwrap();

    let first = queue.peek(0).unwrap().expect("peek");
    assert_eq!(
        first,
        (Bytes::from_static(b"k1"), Bytes::from_static(b"v1"))
    );

    let batch = queue.peek_batch(0, Some(2)).unwrap();
    assert_eq!(
        batch,
        vec![
            (Bytes::from_static(b"k1"), Bytes::from_static(b"v1")),
            (Bytes::from_static(b"k2"), Bytes::from_static(b"v2")),
        ]
    );

    let read_options = StructuredReadOptions::for_column(0).with_column_family("jobs");
    assert!(
        db.get_raw_with_options(0, b"k1", &read_options)
            .unwrap()
            .is_some()
    );
    assert!(
        db.get_raw_with_options(0, b"k2", &read_options)
            .unwrap()
            .is_some()
    );

    let first_polled = queue.poll(0).unwrap().expect("poll after peek");
    assert_eq!(
        first_polled,
        (Bytes::from_static(b"k1"), Bytes::from_static(b"v1"))
    );

    db.close().unwrap();
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn test_priority_queue_advance_to_skips_entries_monotonically() {
    let root = format!("/tmp/ds_priority_queue_advance_{}", Uuid::new_v4());
    let mut db = open_test_db(&root).unwrap();
    db.new_priority_queue("jobs").unwrap();
    let queue = db.get_priority_queue("jobs").unwrap();
    queue.offer(0, b"k1", b"v1").unwrap();
    queue.offer(0, b"k2", b"v2").unwrap();
    queue.offer(0, b"k3", b"v3").unwrap();

    queue.advance_to(0, b"k2").unwrap();
    queue.advance_to(0, b"k1").unwrap();

    let read_options = StructuredReadOptions::for_column(0).with_column_family("jobs");
    assert!(
        db.get_raw_with_options(0, b"k1", &read_options)
            .unwrap()
            .is_none()
    );
    assert!(
        db.get_raw_with_options(0, b"k2", &read_options)
            .unwrap()
            .is_none()
    );
    assert!(
        db.get_raw_with_options(0, b"k3", &read_options)
            .unwrap()
            .is_some()
    );

    let remaining = queue.poll(0).unwrap().expect("poll after advance");
    assert_eq!(
        remaining,
        (Bytes::from_static(b"k3"), Bytes::from_static(b"v3"))
    );
    assert!(queue.poll(0).unwrap().is_none());

    db.close().unwrap();
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn test_priority_queue_poll_batch_without_size_keeps_unbounded_sources_unlimited() {
    let root = format!("/tmp/ds_priority_queue_dynamic_batch_{}", Uuid::new_v4());
    let mut db = open_test_db(&root).unwrap();
    db.new_priority_queue("jobs").unwrap();
    let queue = db.get_priority_queue("jobs").unwrap();
    queue.offer(0, b"k1", b"v1").unwrap();
    queue.offer(0, b"k2", b"v2").unwrap();

    let batch = queue.poll_batch(0, None).unwrap();
    assert_eq!(
        batch,
        vec![
            (Bytes::from_static(b"k1"), Bytes::from_static(b"v1")),
            (Bytes::from_static(b"k2"), Bytes::from_static(b"v2")),
        ]
    );
    assert!(queue.poll(0).unwrap().is_none());
    db.close().unwrap();
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn test_priority_queue_peek_batch_without_size_reads_one_sst_block_without_advancing() {
    let root = format!(
        "/tmp/ds_priority_queue_dynamic_sst_peek_batch_{}",
        Uuid::new_v4()
    );
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        num_columns: 1,
        snapshot_on_flush: true,
        ..Config::default()
    };
    let db_id = {
        let mut db = StructuredDb::open(config.clone(), vec![0u16..=0u16]).unwrap();
        let queue = db.get_or_new_priority_queue("jobs").unwrap();
        queue.offer(0, b"k1", b"v1").unwrap();
        queue.offer(0, b"k2", b"v2").unwrap();
        queue.offer(0, b"k3", b"v3").unwrap();
        let _ = db.snapshot().unwrap();
        thread::sleep(Duration::from_millis(250));
        let db_id = db.id().to_string();
        db.close().unwrap();
        db_id
    };

    let resumed = StructuredDb::resume(config, db_id).unwrap();
    let queue = resumed.get_priority_queue("jobs").unwrap();
    let batch = queue.peek_batch(0, None).unwrap();
    assert_eq!(
        batch,
        vec![
            (Bytes::from_static(b"k1"), Bytes::from_static(b"v1")),
            (Bytes::from_static(b"k2"), Bytes::from_static(b"v2")),
            (Bytes::from_static(b"k3"), Bytes::from_static(b"v3")),
        ]
    );
    let first = queue.poll(0).unwrap().expect("poll after peek batch");
    assert_eq!(
        first,
        (Bytes::from_static(b"k1"), Bytes::from_static(b"v1"))
    );
    resumed.close().unwrap();
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn test_priority_queue_poll_batch_without_size_reads_one_sst_block() {
    let root = format!(
        "/tmp/ds_priority_queue_dynamic_sst_batch_{}",
        Uuid::new_v4()
    );
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        num_columns: 1,
        snapshot_on_flush: true,
        ..Config::default()
    };
    let db_id = {
        let mut db = StructuredDb::open(config.clone(), vec![0u16..=0u16]).unwrap();
        let queue = db.get_or_new_priority_queue("jobs").unwrap();
        queue.offer(0, b"k1", b"v1").unwrap();
        queue.offer(0, b"k2", b"v2").unwrap();
        queue.offer(0, b"k3", b"v3").unwrap();
        let _ = db.snapshot().unwrap();
        thread::sleep(Duration::from_millis(250));
        let db_id = db.id().to_string();
        db.close().unwrap();
        db_id
    };

    let resumed = StructuredDb::resume(config, db_id).unwrap();
    let queue = resumed.get_priority_queue("jobs").unwrap();
    let batch = queue.poll_batch(0, None).unwrap();
    assert_eq!(
        batch,
        vec![
            (Bytes::from_static(b"k1"), Bytes::from_static(b"v1")),
            (Bytes::from_static(b"k2"), Bytes::from_static(b"v2")),
            (Bytes::from_static(b"k3"), Bytes::from_static(b"v3")),
        ]
    );
    assert!(queue.poll(0).unwrap().is_none());
    resumed.close().unwrap();
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn test_new_priority_queue_rejects_non_queue_family() {
    let root = format!("/tmp/ds_priority_queue_reject_family_{}", Uuid::new_v4());
    let mut db = open_test_db(&root).unwrap();
    db.update_schema()
        .add_bytes_column(Some("plain-family".to_string()), 0)
        .commit()
        .unwrap();

    let err = match db.get_or_new_priority_queue("plain-family") {
        Ok(_) => panic!("plain family must not reopen as queue"),
        Err(err) => err,
    };
    assert!(
        matches!(err, Error::InvalidState(message) if message.contains("not marked as a priority queue"))
    );

    db.close().unwrap();
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn test_new_priority_queue_rejects_existing_queue_family() {
    let root = format!("/tmp/ds_priority_queue_duplicate_family_{}", Uuid::new_v4());
    let mut db = open_test_db(&root).unwrap();
    db.new_priority_queue("jobs").unwrap();

    let err = match db.new_priority_queue("jobs") {
        Ok(_) => panic!("new_priority_queue must fail on existing family"),
        Err(err) => err,
    };
    assert!(matches!(err, Error::InvalidState(message) if message.contains("already exists")));

    db.close().unwrap();
    let _ = std::fs::remove_dir_all(root);
}
