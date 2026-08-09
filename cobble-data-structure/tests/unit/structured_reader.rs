use super::*;
use crate::StructuredColumnType;
use crate::list::{ListConfig, ListRetainMode};
use crate::structured_single_db::StructuredSingleDb;
use bytes::Bytes;
use cobble::VolumeDescriptor;
use std::collections::BTreeMap;
use std::thread;
use std::time::Duration;
use uuid::Uuid;

fn test_schema() -> StructuredSchema {
    StructuredSchema {
        column_families: BTreeMap::from([(
            0,
            crate::StructuredColumnFamilySchema {
                columns: BTreeMap::from([(
                    1,
                    StructuredColumnType::List(ListConfig {
                        max_elements: Some(3),
                        retain_mode: ListRetainMode::Last,
                        preserve_element_ttl: false,
                    }),
                )]),
            },
        )]),
        ..Default::default()
    }
}

fn test_config(root: &str) -> cobble::Config {
    cobble::Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        num_columns: 1,
        total_buckets: 2,
        snapshot_on_flush: true,
        ..cobble::Config::default()
    }
}

#[test]
fn test_structured_reader_get_scan() {
    let root = format!("/tmp/ds_reader_get_scan_{}", Uuid::new_v4());

    // Write data via StructuredSingleDb and create a global snapshot
    let mut db = StructuredSingleDb::open(test_config(&root)).unwrap();
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
    db.put(0, b"k1", 0, Bytes::from_static(b"v0")).unwrap();
    db.merge(0, b"k1", 1, vec![Bytes::from_static(b"a")])
        .unwrap();
    db.merge(0, b"k1", 1, vec![Bytes::from_static(b"b")])
        .unwrap();
    let snap_id = db.snapshot().unwrap();
    thread::sleep(Duration::from_millis(200));
    db.close().unwrap();

    // Open as StructuredReader
    let read_config = ReaderConfig {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        total_buckets: 2,
        ..ReaderConfig::default()
    };
    let mut reader = StructuredReader::open(read_config, snap_id).unwrap();

    // Verify schema was auto-loaded
    assert_eq!(reader.current_schema(), test_schema());

    // get
    let row = reader.get(0, b"k1").unwrap().expect("row exists");
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

    // scan
    let mut iter = reader.scan(0, b"k0".as_ref()..b"k9".as_ref()).unwrap();
    let first = iter.next().expect("one row").unwrap();
    assert_eq!(first.0.as_ref(), b"k1");
    assert!(iter.next().is_none());

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn test_structured_reader_open_current() {
    let root = format!("/tmp/ds_reader_current_{}", Uuid::new_v4());

    let mut db = StructuredSingleDb::open(test_config(&root)).unwrap();
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
    db.put(0, b"k1", 0, Bytes::from_static(b"v0")).unwrap();
    let _ = db.snapshot().unwrap();
    thread::sleep(Duration::from_millis(200));
    db.close().unwrap();

    let read_config = ReaderConfig {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        total_buckets: 2,
        ..ReaderConfig::default()
    };
    let mut reader = StructuredReader::open_current(read_config).unwrap();

    let row = reader.get(0, b"k1").unwrap().expect("row exists");
    assert_eq!(
        row[0],
        Some(StructuredColumnValue::Bytes(Bytes::from_static(b"v0")))
    );

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn test_structured_reader_get_with_projection_reindexes_schema() {
    let root = format!("/tmp/ds_reader_get_projection_{}", Uuid::new_v4());

    let mut db = StructuredSingleDb::open(test_config(&root)).unwrap();
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
    db.put(0, b"k1", 0, Bytes::from_static(b"v0")).unwrap();
    db.merge(0, b"k1", 1, vec![Bytes::from_static(b"a")])
        .unwrap();
    db.merge(0, b"k1", 1, vec![Bytes::from_static(b"b")])
        .unwrap();
    let snap_id = db.snapshot().unwrap();
    thread::sleep(Duration::from_millis(200));
    db.close().unwrap();

    let read_config = ReaderConfig {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        total_buckets: 2,
        ..ReaderConfig::default()
    };
    let mut reader = StructuredReader::open(read_config, snap_id).unwrap();
    let row = reader
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

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn test_structured_reader_column_family_get_scan() {
    let root = format!("/tmp/ds_reader_cf_{}", Uuid::new_v4());
    let metrics_config = ListConfig::default();

    let mut db = StructuredSingleDb::open(test_config(&root)).unwrap();
    db.update_schema()
        .add_list_column(Some("metrics".to_string()), 0, metrics_config.clone())
        .commit()
        .unwrap();
    let metrics_write = crate::StructuredWriteOptions::with_column_family("metrics");
    db.put_with_options(0, b"k1", 0, vec![Bytes::from_static(b"a")], &metrics_write)
        .unwrap();
    db.merge_with_options(0, b"k1", 0, vec![Bytes::from_static(b"b")], &metrics_write)
        .unwrap();
    let snap_id = db.snapshot().unwrap();
    thread::sleep(Duration::from_millis(200));
    db.close().unwrap();

    let read_config = ReaderConfig {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        total_buckets: 2,
        ..ReaderConfig::default()
    };
    let mut reader = StructuredReader::open(read_config, snap_id).unwrap();
    assert!(reader.current_schema().column_families.contains_key(&1));

    let row = reader
        .get_with_options(
            0,
            b"k1",
            &StructuredReadOptions::for_column_in_family("metrics", 0),
        )
        .unwrap()
        .expect("row exists");
    assert_eq!(
        row[0],
        Some(StructuredColumnValue::List(vec![
            Bytes::from_static(b"a"),
            Bytes::from_static(b"b"),
        ]))
    );

    let mut iter = reader
        .scan_with_options(
            0,
            b"k0".as_ref()..b"k9".as_ref(),
            &StructuredScanOptions::for_column(0).with_column_family("metrics"),
        )
        .unwrap();
    let first = iter.next().expect("one row").unwrap();
    assert_eq!(first.0.as_ref(), b"k1");
    assert_eq!(
        first.1[0],
        Some(StructuredColumnValue::List(vec![
            Bytes::from_static(b"a"),
            Bytes::from_static(b"b"),
        ]))
    );
    assert!(iter.next().is_none());

    let _ = std::fs::remove_dir_all(root);
}
