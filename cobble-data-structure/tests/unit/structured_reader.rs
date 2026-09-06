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
fn test_structured_reader_transforms_follow_snapshot_schema() {
    use crate::list::decode_list_for_read;
    use std::sync::mpsc;

    fn list_to_bytes(value: Option<Bytes>) -> Result<Option<Bytes>> {
        value
            .map(|raw| {
                let elements = decode_list_for_read(&raw, &ListConfig::default(), 0)?;
                Ok(Bytes::from(elements.concat()))
            })
            .transpose()
    }
    fn uppercase(value: Option<Bytes>) -> Result<Option<Bytes>> {
        Ok(value.map(|raw| Bytes::from(raw.to_ascii_uppercase())))
    }
    fn snapshot(db: &StructuredSingleDb) -> u64 {
        let (tx, rx) = mpsc::channel();
        let id = db
            .snapshot_with_callback(move |result| tx.send(result).unwrap())
            .unwrap();
        rx.recv_timeout(Duration::from_secs(30)).unwrap().unwrap();
        id
    }

    let root = format!("/tmp/ds_reader_transform_{}", Uuid::new_v4());
    let mut config = test_config(&root);
    config.l0_file_limit = 1000;
    let mut db = StructuredSingleDb::open(config.clone()).unwrap();
    db.update_schema()
        .add_list_column(None, 1, ListConfig::default())
        .commit()
        .unwrap();
    db.put(0, b"k", 1, vec![Bytes::from_static(b"a")]).unwrap();
    let initial = snapshot(&db);
    let read_config = ReaderConfig {
        reload_tolerance: Duration::ZERO,
        ..ReaderConfig::from_config(&config)
    };
    let mut reader = StructuredReaderBuilder::new(read_config.clone())
        .register_schema_transform("list-to-bytes", list_to_bytes)
        .unwrap()
        .open_current()
        .unwrap();
    let options = StructuredReadOptions::for_column(1);
    assert_eq!(
        reader.get_with_options(0, b"k", &options).unwrap().unwrap(),
        vec![Some(StructuredColumnValue::List(vec![Bytes::from_static(
            b"a"
        )]))]
    );

    db.register_schema_transform("list-to-bytes", list_to_bytes)
        .unwrap();
    db.update_schema()
        .transform_column(None, 1, StructuredColumnType::Bytes, "list-to-bytes")
        .commit()
        .unwrap();
    db.merge(0, b"k", 1, b"b".to_vec()).unwrap();
    // Local CURRENT polling uses second-granularity mtime. Publish in a new
    // second so this exercises normal auto-refresh, not a forced refresh.
    thread::sleep(Duration::from_millis(1100));
    let evolved = snapshot(&db);
    // multi_get auto-refreshes; its typed projection must be resolved after refresh.
    let expected = vec![
        None,
        Some(StructuredColumnValue::Bytes(Bytes::from_static(b"ab"))),
    ];
    assert_eq!(
        reader
            .multi_get_with_options(&[(0, b"k".as_slice())], &options)
            .unwrap(),
        vec![Some(vec![expected[1].clone()])]
    );
    assert_eq!(reader.current_global_snapshot().id, evolved);
    assert_eq!(reader.current_schema(), db.current_schema());
    assert_eq!(
        reader
            .scan(0, b"k".as_slice()..b"l".as_slice())
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .1,
        expected
    );
    // A fixed reader continues decoding the original List schema.
    let mut fixed = StructuredReader::open(read_config.clone(), initial).unwrap();
    assert!(matches!(
        fixed.get_with_options(0, b"k", &options).unwrap().unwrap()[0],
        Some(StructuredColumnValue::List(_))
    ));

    db.register_schema_transform("uppercase", uppercase)
        .unwrap();
    db.update_schema()
        .transform_column(None, 1, StructuredColumnType::Bytes, "uppercase")
        .commit()
        .unwrap();
    thread::sleep(Duration::from_millis(1100));
    let latest = snapshot(&db);
    assert!(reader.get(0, b"k").is_err()); // Callback is not installed on this reader yet.
    reader
        .register_schema_transform("uppercase", uppercase)
        .unwrap();
    reader.refresh().unwrap();
    assert_eq!(reader.current_global_snapshot().id, latest);
    assert_eq!(
        reader.get(0, b"k").unwrap().unwrap()[1],
        Some(StructuredColumnValue::Bytes(Bytes::from_static(b"AB")))
    );
    drop(reader);
    drop(fixed);
    // Startup registration reaches both the lazy shard reader and schema bootstrap.
    let mut reopened = StructuredReaderBuilder::new(read_config)
        .register_schema_transform("list-to-bytes", list_to_bytes)
        .unwrap()
        .register_schema_transform("uppercase", uppercase)
        .unwrap()
        .open(latest)
        .unwrap();
    assert_eq!(reopened.current_schema(), db.current_schema());
    assert_eq!(
        reopened.get(0, b"k").unwrap().unwrap()[1],
        Some(StructuredColumnValue::Bytes(Bytes::from_static(b"AB")))
    );
    drop(reopened);
    db.close().unwrap();
    drop(db);
    std::fs::remove_dir_all(root).unwrap();
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
