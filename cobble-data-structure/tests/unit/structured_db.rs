use super::*;
use crate::list::{ListConfig, ListRetainMode};
use cobble::VolumeDescriptor;
use std::thread;
use std::time::Duration;
use uuid::Uuid;

fn default_family_schema(columns: BTreeMap<u16, StructuredColumnType>) -> StructuredSchema {
    StructuredSchema {
        column_families: BTreeMap::from([(
            DEFAULT_COLUMN_FAMILY_ID,
            StructuredColumnFamilySchema { columns },
        )]),
        ..Default::default()
    }
}

#[test]
fn test_structured_list_and_bytes_transforms_keep_layout_and_recovery_in_sync() {
    use crate::StructuredReadOnlyDbBuilder;
    use std::sync::mpsc;

    fn uppercase(value: Option<Bytes>) -> Result<Option<Bytes>> {
        Ok(value.map(|bytes| Bytes::from(bytes.to_ascii_uppercase())))
    }
    fn uppercase_list() -> impl Fn(Option<Bytes>) -> Result<Option<Bytes>> + Send + Sync {
        StructuredColumnType::list_element_transform(ListConfig::default(), |element| {
            Ok(Bytes::from(element.to_ascii_uppercase()))
        })
    }
    fn snapshot(db: &StructuredDb) -> u64 {
        let (tx, rx) = mpsc::channel();
        let id = db
            .snapshot_with_callback(move |result| tx.send(result).unwrap())
            .unwrap();
        rx.recv_timeout(Duration::from_secs(30)).unwrap().unwrap();
        id
    }

    let root = format!("/tmp/ds_column_transforms_{}", Uuid::new_v4());
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{root}")),
        num_columns: 2,
        l0_file_limit: 1000,
        ..Config::default()
    };
    let mut db = StructuredDbBuilder::new(config.clone())
        .bucket_ranges(vec![0..=0])
        .register_schema_transform("uppercase", |_spec| Ok(uppercase))
        .unwrap()
        .open()
        .unwrap();
    db.register_schema_transform("list-uppercase-v1", |_spec| Ok(uppercase_list()))
        .unwrap();
    db.update_schema()
        .add_list_column(None, 1, ListConfig::default())
        .add_bytes_column(Some("named".into()), 0)
        .set_column_family_options(Some("empty".into()), cobble::ColumnFamilyOptions::default())
        .commit()
        .unwrap();
    db.put(0, b"k", 0, b"alpha".to_vec()).unwrap();
    db.put(0, b"k", 2, b"removed".to_vec()).unwrap();
    db.merge(0, b"k", 1, vec![Bytes::from_static(b"a")])
        .unwrap();
    let named_write = StructuredWriteOptions::with_column_family("named");
    db.put_with_options(0, b"k", 0, b"named".to_vec(), &named_write)
        .unwrap();
    snapshot(&db); // Leave old-schema SSTs, then merge another operand in memory.
    db.merge(0, b"k", 1, vec![Bytes::from_static(b"b")])
        .unwrap();

    let projected = StructuredReadOptions::for_column(1);
    assert_eq!(
        db.get_with_options(0, b"k", &projected).unwrap().unwrap(),
        vec![Some(StructuredColumnValue::List(vec![
            Bytes::from_static(b"a"),
            Bytes::from_static(b"b"),
        ]))]
    );
    let old_schema = db.current_schema();
    let old_core_schema = db.db.current_schema();
    assert!(
        db.update_schema()
            .add_bytes_column(None, 0)
            .transform_column(None, 2, StructuredColumnType::Bytes, "missing")
            .commit()
            .is_err()
    );
    assert_eq!(db.current_schema(), old_schema);
    assert!(Arc::ptr_eq(&db.db.current_schema(), &old_core_schema));

    let list_config = ListConfig {
        max_elements: Some(3),
        ..ListConfig::default()
    };
    db.update_schema()
        .add_bytes_column(None, 0)
        .delete_column(None, 3)
        .transform_column(None, 1, StructuredColumnType::Bytes, "uppercase")
        .transform_column(
            None,
            2,
            StructuredColumnType::List(list_config.clone()),
            "list-uppercase-v1",
        )
        .transform_column(
            Some("named".into()),
            0,
            StructuredColumnType::Bytes,
            "uppercase",
        )
        .commit()
        .unwrap();
    assert_eq!(
        db.current_schema(),
        load_structured_schema_from_cobble_schema(&db.db.current_schema()).unwrap()
    );
    assert_eq!(db.db.current_schema().num_columns(), 3);
    let mut expected = vec![
        None,
        Some(StructuredColumnValue::Bytes(Bytes::from_static(b"ALPHA"))),
        Some(StructuredColumnValue::List(vec![
            Bytes::from_static(b"A"),
            Bytes::from_static(b"B"),
        ])),
    ];
    assert_eq!(db.get(0, b"k").unwrap().unwrap(), expected);
    // Reusing a cached projection must observe the new index/type, not the old List.
    assert_eq!(
        db.get_with_options(0, b"k", &projected).unwrap().unwrap(),
        vec![expected[1].clone()]
    );
    let named_read = StructuredReadOptions::default().with_column_family("named");
    assert_eq!(
        db.get_with_options(0, b"k", &named_read).unwrap().unwrap(),
        vec![Some(StructuredColumnValue::Bytes(Bytes::from_static(
            b"NAMED"
        )))]
    );
    // This migrates old-schema data, not every future write. New operands use
    // the target List operator after the old epoch has been transformed.
    db.merge(0, b"k", 2, vec![Bytes::from_static(b"c")])
        .unwrap();
    expected[2] = Some(StructuredColumnValue::List(vec![
        Bytes::from_static(b"A"),
        Bytes::from_static(b"B"),
        Bytes::from_static(b"c"),
    ]));
    assert_eq!(
        db.multi_get(&[(0, b"k".as_slice()), (0, b"missing".as_slice())])
            .unwrap(),
        vec![Some(expected.clone()), None]
    );
    assert_eq!(
        db.scan(0, b"k".as_slice()..b"l".as_slice())
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .1,
        expected
    );

    let snapshot_id = snapshot(&db);
    let db_id = db.id().to_string();
    let target_schema = db.current_schema();
    db.close().unwrap();
    drop(db);
    assert!(
        StructuredReadOnlyDbBuilder::new(config.clone())
            .db_id(&db_id)
            .open(snapshot_id)
            .is_err()
    );
    let readonly = StructuredReadOnlyDbBuilder::new(config.clone())
        .db_id(&db_id)
        .register_schema_transform("uppercase", |_spec| Ok(uppercase))
        .unwrap()
        .register_schema_transform("list-uppercase-v1", |_spec| Ok(uppercase_list()))
        .unwrap()
        .open(snapshot_id)
        .unwrap();
    assert_eq!(readonly.current_schema(), target_schema);
    assert_eq!(readonly.get(0, b"k").unwrap().unwrap(), expected);
    assert_eq!(
        readonly
            .scan(0, b"k".as_slice()..b"l".as_slice())
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .1,
        expected
    );
    drop(readonly);
    let resumed = StructuredDbBuilder::new(config)
        .db_id(&db_id)
        .register_schema_transform("uppercase", |_spec| Ok(uppercase))
        .unwrap()
        .register_schema_transform("list-uppercase-v1", |_spec| Ok(uppercase_list()))
        .unwrap()
        .resume_from_snapshot(snapshot_id)
        .unwrap();
    assert_eq!(resumed.current_schema(), target_schema);
    assert_eq!(resumed.get(0, b"k").unwrap().unwrap(), expected);
    resumed.close().unwrap();
    drop(resumed);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn test_structured_db_resume_loads_structured_schema() {
    let root = format!("/tmp/ds_structured_resume_{}", Uuid::new_v4());
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        snapshot_on_flush: true,
        num_columns: 2,
        ..Config::default()
    };
    let structured_schema = default_family_schema(BTreeMap::from([(
        1,
        StructuredColumnType::List(ListConfig {
            max_elements: Some(2),
            retain_mode: ListRetainMode::Last,
            preserve_element_ttl: true,
        }),
    )]));
    let mut db = StructuredDb::open(config.clone(), vec![0u16..=0u16]).unwrap();
    db.apply_schema(structured_schema.clone()).unwrap();
    db.merge(0, b"k", 1, vec![Bytes::from_static(b"a")])
        .unwrap();
    let _ = db.snapshot().unwrap();
    thread::sleep(Duration::from_millis(200));
    let db_id = db.id().to_string();
    db.close().unwrap();

    let resumed = StructuredDb::resume(config, db_id).unwrap();
    assert_eq!(resumed.current_schema(), structured_schema);
    resumed.close().unwrap();
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn test_structured_db_get_and_scan_return_structured_values() {
    let root = format!("/tmp/ds_structured_get_scan_{}", Uuid::new_v4());
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        num_columns: 2,
        ..Config::default()
    };
    let structured_schema = default_family_schema(BTreeMap::from([(
        1,
        StructuredColumnType::List(ListConfig {
            max_elements: Some(2),
            retain_mode: ListRetainMode::Last,
            preserve_element_ttl: false,
        }),
    )]));
    let mut db = StructuredDb::open(config, vec![0u16..=0u16]).unwrap();
    db.apply_schema(structured_schema).unwrap();
    db.put(0, b"k1", 0, Bytes::from_static(b"v0")).unwrap();
    db.merge(0, b"k1", 1, vec![Bytes::from_static(b"a")])
        .unwrap();
    db.merge(0, b"k1", 1, vec![Bytes::from_static(b"b")])
        .unwrap();
    db.merge(0, b"k1", 1, vec![Bytes::from_static(b"c")])
        .unwrap();

    let row = db.get(0, b"k1").unwrap().expect("row exists");
    assert_eq!(
        row[0],
        Some(StructuredColumnValue::Bytes(Bytes::from_static(b"v0")))
    );
    assert_eq!(
        row[1],
        Some(StructuredColumnValue::List(vec![
            Bytes::from_static(b"b"),
            Bytes::from_static(b"c")
        ]))
    );

    let mut iter = db.scan(0, b"k0".as_ref()..b"k9".as_ref()).unwrap();
    let first = iter.next().expect("one row").unwrap();
    assert_eq!(first.0.as_ref(), b"k1");
    assert_eq!(first.1.len(), 2, "scan row should have 2 columns");
    assert_eq!(
        first.1[0],
        Some(StructuredColumnValue::Bytes(Bytes::from_static(b"v0")))
    );
    assert_eq!(
        first.1[1],
        Some(StructuredColumnValue::List(vec![
            Bytes::from_static(b"b"),
            Bytes::from_static(b"c")
        ]))
    );
    assert!(iter.next().is_none());
    drop(iter);

    db.close().unwrap();
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn test_structured_write_batch_round_trip() {
    let root = format!("/tmp/ds_structured_write_batch_{}", Uuid::new_v4());
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        num_columns: 2,
        ..Config::default()
    };
    let structured_schema = default_family_schema(BTreeMap::from([(
        1,
        StructuredColumnType::List(ListConfig {
            max_elements: Some(3),
            retain_mode: ListRetainMode::Last,
            preserve_element_ttl: false,
        }),
    )]));
    let mut db = StructuredDb::open(config, vec![0u16..=0u16]).unwrap();
    db.apply_schema(structured_schema).unwrap();
    let mut batch = db.new_write_batch();
    batch.put(0, b"k1", 0, Bytes::from_static(b"v0")).unwrap();
    batch
        .merge(0, b"k1", 1, vec![Bytes::from_static(b"a")])
        .unwrap();
    batch
        .merge(0, b"k1", 1, vec![Bytes::from_static(b"b")])
        .unwrap();
    batch
        .merge(0, b"k1", 1, vec![Bytes::from_static(b"c")])
        .unwrap();
    batch.put(0, b"k2", 0, Bytes::from_static(b"v2")).unwrap();
    db.write_batch(batch).unwrap();

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
            Bytes::from_static(b"c")
        ]))
    );
    let mut iter = db.scan(0, b"k0".as_ref()..b"k9".as_ref()).unwrap();
    let first = iter.next().expect("first row").unwrap();
    assert_eq!(first.0.as_ref(), b"k1");
    let second = iter.next().expect("second row").unwrap();
    assert_eq!(second.0.as_ref(), b"k2");
    assert!(iter.next().is_none());
    drop(iter);

    db.close().unwrap();
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn test_structured_write_batch_rejects_type_mismatch() {
    let root = format!("/tmp/ds_structured_write_batch_mismatch_{}", Uuid::new_v4());
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        num_columns: 2,
        ..Config::default()
    };
    let structured_schema = default_family_schema(BTreeMap::from([(
        1,
        StructuredColumnType::List(ListConfig {
            max_elements: None,
            retain_mode: ListRetainMode::Last,
            preserve_element_ttl: false,
        }),
    )]));
    let mut db = StructuredDb::open(config, vec![0u16..=0u16]).unwrap();
    db.apply_schema(structured_schema).unwrap();
    let mut batch = db.new_write_batch();
    let err = batch
        .put(0, b"k1", 1, Bytes::from_static(b"not-a-list"))
        .expect_err("type mismatch should fail");
    match err {
        Error::InputError(msg) => assert!(msg.contains("column 1 expects")),
        other => panic!("unexpected error: {other:?}"),
    }
    db.close().unwrap();
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn test_structured_scan_with_projection_reindexes_schema() {
    let root = format!("/tmp/ds_structured_scan_projection_{}", Uuid::new_v4());
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        num_columns: 2,
        ..Config::default()
    };
    let structured_schema = default_family_schema(BTreeMap::from([(
        1,
        StructuredColumnType::List(ListConfig {
            max_elements: Some(8),
            retain_mode: ListRetainMode::Last,
            preserve_element_ttl: false,
        }),
    )]));
    let mut db = StructuredDb::open(config, vec![0u16..=0u16]).unwrap();
    db.apply_schema(structured_schema).unwrap();
    db.put(0, b"k1", 0, Bytes::from_static(b"v0")).unwrap();
    db.merge(0, b"k1", 1, vec![Bytes::from_static(b"a")])
        .unwrap();
    db.merge(0, b"k1", 1, vec![Bytes::from_static(b"b")])
        .unwrap();

    let mut iter = db
        .scan_with_options(
            0,
            b"k0".as_ref()..b"k9".as_ref(),
            &StructuredScanOptions::for_column(1),
        )
        .unwrap();
    let first = iter.next().expect("one row").unwrap();
    assert_eq!(first.0.as_ref(), b"k1");
    assert_eq!(first.1.len(), 1);
    assert_eq!(
        first.1[0],
        Some(StructuredColumnValue::List(vec![
            Bytes::from_static(b"a"),
            Bytes::from_static(b"b"),
        ]))
    );
    drop(iter);

    db.close().unwrap();
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn test_structured_get_with_projection_reindexes_schema() {
    let root = format!("/tmp/ds_structured_get_projection_{}", Uuid::new_v4());
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        num_columns: 2,
        ..Config::default()
    };
    let structured_schema = default_family_schema(BTreeMap::from([(
        1,
        StructuredColumnType::List(ListConfig {
            max_elements: Some(8),
            retain_mode: ListRetainMode::Last,
            preserve_element_ttl: false,
        }),
    )]));
    let mut db = StructuredDb::open(config, vec![0u16..=0u16]).unwrap();
    db.apply_schema(structured_schema).unwrap();
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

#[test]
fn test_structured_db_column_family_get_scan_and_write_batch() {
    let root = format!("/tmp/ds_structured_cf_{}", Uuid::new_v4());
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        num_columns: 2,
        ..Config::default()
    };
    let metrics_config = ListConfig {
        max_elements: Some(8),
        retain_mode: ListRetainMode::Last,
        preserve_element_ttl: false,
    };

    let mut db = StructuredDb::open(config, vec![0u16..=0u16]).unwrap();
    let schema = db
        .update_schema()
        .add_list_column(Some("metrics".to_string()), 0, metrics_config.clone())
        .commit()
        .unwrap();
    assert_eq!(
        schema
            .column_families
            .get(&1)
            .and_then(|family| family.columns.get(&0)),
        Some(&StructuredColumnType::List(metrics_config.clone()))
    );

    let metrics_write = StructuredWriteOptions::with_column_family("metrics");
    db.put_with_options(0, b"k1", 0, vec![Bytes::from_static(b"a")], &metrics_write)
        .unwrap();
    db.merge_with_options(0, b"k1", 0, vec![Bytes::from_static(b"b")], &metrics_write)
        .unwrap();

    let row = db
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

    let mut iter = db
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
    drop(iter);

    let mut batch = db.new_write_batch();
    batch
        .put_with_options(0, b"k2", 0, vec![Bytes::from_static(b"c")], &metrics_write)
        .unwrap();
    db.write_batch(batch).unwrap();

    let batch_row = db
        .get_with_options(
            0,
            b"k2",
            &StructuredReadOptions::for_column_in_family("metrics", 0),
        )
        .unwrap()
        .expect("batch row exists");
    assert_eq!(
        batch_row[0],
        Some(StructuredColumnValue::List(vec![Bytes::from_static(b"c")]))
    );

    db.close().unwrap();
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn test_structured_schema_builder_reindexes_family_local_columns_on_add_and_delete() {
    let root = format!("/tmp/ds_structured_builder_indexes_{}", Uuid::new_v4());
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        num_columns: 1,
        ..Config::default()
    };
    let first = ListConfig {
        max_elements: Some(4),
        retain_mode: ListRetainMode::Last,
        preserve_element_ttl: false,
    };
    let second = ListConfig {
        max_elements: Some(6),
        retain_mode: ListRetainMode::First,
        preserve_element_ttl: true,
    };

    let mut db = StructuredDb::open(config, vec![0u16..=0u16]).unwrap();
    let schema = db
        .update_schema()
        .add_list_column(None, 1, second.clone())
        .add_list_column(None, 1, first.clone())
        .commit()
        .unwrap();
    assert_eq!(db.db.current_schema().num_columns(), 3);

    let family = schema
        .column_families
        .get(&0)
        .expect("default family schema");
    assert_eq!(
        family.columns.get(&1),
        Some(&StructuredColumnType::List(first))
    );
    assert_eq!(
        family.columns.get(&2),
        Some(&StructuredColumnType::List(second.clone()))
    );

    let schema = db.update_schema().delete_column(None, 1).commit().unwrap();
    assert_eq!(db.db.current_schema().num_columns(), 2);
    let family = schema
        .column_families
        .get(&0)
        .expect("default family schema");
    assert_eq!(
        family.columns.get(&1),
        Some(&StructuredColumnType::List(second))
    );
    assert!(!family.columns.contains_key(&2));

    db.close().unwrap();
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn test_structured_schema_builder_normalizes_column_family_names_after_inner_success() {
    let root = format!("/tmp/ds_structured_builder_family_name_{}", Uuid::new_v4());
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        num_columns: 1,
        ..Config::default()
    };
    let metrics_config = ListConfig {
        max_elements: Some(5),
        retain_mode: ListRetainMode::Last,
        preserve_element_ttl: false,
    };

    let mut db = StructuredDb::open(config, vec![0u16..=0u16]).unwrap();
    let baseline = db.current_schema();
    assert_eq!(
        baseline
            .column_families()
            .keys()
            .cloned()
            .collect::<Vec<_>>(),
        vec!["default".to_string()]
    );

    let mut invalid_builder = db.update_schema();
    invalid_builder.add_list_column(Some("   ".to_string()), 0, metrics_config.clone());
    assert_eq!(invalid_builder.current_schema(), &baseline);
    let err = invalid_builder
        .commit()
        .expect_err("empty family should fail");
    assert!(matches!(err, Error::InvalidState(msg) if msg.contains("cannot be empty")));

    let schema = db
        .update_schema()
        .add_list_column(Some(" metrics ".to_string()), 0, metrics_config.clone())
        .commit()
        .unwrap();
    assert_eq!(schema.column_family_ids.get("metrics"), Some(&1));
    assert!(!schema.column_family_ids.contains_key(" metrics "));
    assert_eq!(
        schema
            .column_families
            .get(&1)
            .and_then(|family| family.columns.get(&0)),
        Some(&StructuredColumnType::List(metrics_config))
    );
    let named_families = schema.column_families();
    assert!(named_families.contains_key("default"));
    assert_eq!(
        named_families
            .get("metrics")
            .and_then(|family| family.columns.get(&0)),
        Some(&StructuredColumnType::List(ListConfig {
            max_elements: Some(5),
            retain_mode: ListRetainMode::Last,
            preserve_element_ttl: false,
        }))
    );

    db.close().unwrap();
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn test_structured_db_iterator_consume_next_row() {
    let root = format!("/tmp/ds_structured_consume_scan_{}", Uuid::new_v4());
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        num_columns: 2,
        ..Config::default()
    };
    let structured_schema = default_family_schema(BTreeMap::from([(
        1,
        StructuredColumnType::List(ListConfig {
            max_elements: Some(8),
            retain_mode: ListRetainMode::Last,
            preserve_element_ttl: false,
        }),
    )]));
    let mut db = StructuredDb::open(config, vec![0u16..=0u16]).unwrap();
    db.apply_schema(structured_schema).unwrap();
    db.put(0, b"k1", 0, Bytes::from_static(b"v0")).unwrap();
    db.merge(0, b"k1", 1, vec![Bytes::from_static(b"a")])
        .unwrap();
    db.merge(0, b"k1", 1, vec![Bytes::from_static(b"b")])
        .unwrap();
    db.put(0, b"k2", 0, Bytes::from_static(b"v2")).unwrap();

    let mut iter = db
        .scan_with_options(
            0,
            b"k0".as_ref()..b"k9".as_ref(),
            &StructuredScanOptions::default(),
        )
        .unwrap();
    let schema = Arc::clone(&iter.structured_schema);
    let schema_references = Arc::strong_count(&schema);
    let mut rows = Vec::new();
    while let Some(row) = iter
        .consume_next_row_with_bucket(|bucket, key, columns| {
            assert_eq!(Arc::strong_count(&schema), schema_references);
            Ok((bucket, key.clone(), columns.to_vec()))
        })
        .unwrap()
    {
        rows.push(row);
    }

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].0, 0);
    assert_eq!(rows[0].1.as_ref(), b"k1");
    assert_eq!(
        rows[0].2[1],
        Some(StructuredColumnValue::List(vec![
            Bytes::from_static(b"a"),
            Bytes::from_static(b"b"),
        ]))
    );
    assert_eq!(rows[1].0, 0);
    assert_eq!(rows[1].1.as_ref(), b"k2");
    assert_eq!(
        rows[1].2[0],
        Some(StructuredColumnValue::Bytes(Bytes::from_static(b"v2")))
    );

    drop(iter);
    db.close().unwrap();
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn test_structured_owned_row_consumption_preserves_error_limits_and_projection() {
    let root = format!("/tmp/ds_structured_owned_consume_{}", Uuid::new_v4());
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{root}")),
        num_columns: 2,
        ..Config::default()
    };
    let mut db = StructuredDb::open(config, vec![0..=0]).unwrap();
    db.apply_schema(default_family_schema(BTreeMap::from([(
        1,
        StructuredColumnType::List(ListConfig::default()),
    )])))
    .unwrap();
    db.db.put(0, b"k1", 1, b"invalid list").unwrap();
    for key in [b"k2", b"k3"] {
        db.put(0, key, 0, Bytes::from_static(b"value")).unwrap();
        db.merge(0, key, 1, vec![Bytes::from_static(b"element")])
            .unwrap();
    }
    let (tx, rx) = std::sync::mpsc::channel();
    db.snapshot_with_callback(move |result| tx.send(result).unwrap())
        .unwrap();
    rx.recv_timeout(Duration::from_secs(30)).unwrap().unwrap();
    let options = StructuredScanOptions::from(ScanOptions::for_column(1).with_max_rows(1));

    {
        let mut iter = db.scan_with_options(0, b"k0"..b"k9", &options).unwrap();
        assert!(iter.consume_next_row(|_, _| Ok(())).is_err());
        let schema = Arc::clone(&iter.structured_schema);
        let schema_references = Arc::strong_count(&schema);
        let row = iter
            .consume_next_row(|key, columns| {
                assert_eq!(Arc::strong_count(&schema), schema_references);
                Ok((key.clone(), columns.to_vec()))
            })
            .unwrap()
            .unwrap();
        assert_eq!(row.0.as_ref(), b"k2");
        assert_eq!(
            row.1,
            vec![Some(StructuredColumnValue::List(vec![Bytes::from_static(
                b"element"
            )]))]
        );
        assert!(iter.consume_next_row(|_, _| Ok(())).unwrap().is_none());

        // The malformed row is already advanced, but non-bucket callback errors
        // still leave the successful-row budget available for the next row.
        let mut iter = db.scan_with_options(0, b"k2"..b"k9", &options).unwrap();
        assert!(
            iter.consume_next_row::<(), _>(|_, _| Err(Error::InputError(
                "consumer failed".to_string()
            )))
            .is_err()
        );
        assert_eq!(
            iter.consume_next_row(|key, _| Ok(key.clone()))
                .unwrap()
                .unwrap()
                .as_ref(),
            b"k3"
        );
        assert!(iter.next().is_none());

        let mut iter = db.scan_with_options(0, b"k0"..b"k9", &options).unwrap();
        assert!(iter.consume_next_row_with_bucket(|_, _, _| Ok(())).is_err());
        assert!(
            iter.consume_next_row_with_bucket(|_, _, _| Ok(()))
                .unwrap()
                .is_none()
        );
        let mut iter = db.scan_with_options(0, b"k2"..b"k9", &options).unwrap();
        assert!(
            iter.consume_next_row_with_bucket::<(), _>(|_, _, _| {
                Err(Error::InputError("consumer failed".to_string()))
            })
            .is_err()
        );
        assert!(iter.next().is_none());
    }
    db.close().unwrap();
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn test_structured_owned_consumption_resumes_at_sst_block_boundaries() {
    let root = format!("/tmp/ds_structured_owned_blocks_{}", Uuid::new_v4());
    let mut db = StructuredDb::open(
        Config {
            volumes: VolumeDescriptor::single_volume(format!("file://{root}")),
            num_columns: 2,
            ..Config::default()
        },
        vec![0..=0],
    )
    .unwrap();
    db.apply_schema(default_family_schema(BTreeMap::from([(
        1,
        StructuredColumnType::List(ListConfig::default()),
    )])))
    .unwrap();
    let element = Bytes::from(vec![b'x'; 2048]);
    for i in 0..128 {
        db.merge(0, format!("k{i:03}"), 1, vec![element.clone()])
            .unwrap();
    }
    let (tx, rx) = std::sync::mpsc::channel();
    db.snapshot_with_callback(move |result| tx.send(result).unwrap())
        .unwrap();
    rx.recv_timeout(Duration::from_secs(30)).unwrap().unwrap();

    let options = StructuredScanOptions::for_column(1).with_stop_at_block_boundary(true);
    let mut iter = db.scan_with_options(0, b"k"..b"l", &options).unwrap();
    let mut rows = 0;
    let mut pauses = 0;
    loop {
        let check_row = |key: &Bytes, columns: &[Option<StructuredColumnValue>]| {
            assert_eq!(key.as_ref(), format!("k{rows:03}").as_bytes());
            assert_eq!(
                columns,
                &[Some(StructuredColumnValue::List(vec![element.clone()]))]
            );
            Ok(())
        };
        let row = if rows % 2 == 0 {
            iter.consume_next_row(check_row)
        } else {
            iter.consume_next_row_with_bucket(|bucket, key, columns| {
                assert_eq!(bucket, 0);
                check_row(key, columns)
            })
        }
        .unwrap();
        if row.is_some() {
            rows += 1;
        } else if iter.inner.stopped_at_block_boundary() {
            pauses += 1;
            assert!(pauses < 128, "scan must make progress");
            iter.inner.clear_stop_at_block_boundary();
        } else {
            break;
        }
    }
    assert_eq!(rows, 128);
    assert!(pauses > 0);
    assert!(iter.consume_next_row(|_, _| Ok(())).unwrap().is_none());
    drop(iter);
    db.close().unwrap();
    let _ = std::fs::remove_dir_all(root);
}
