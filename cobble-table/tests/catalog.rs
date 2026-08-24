use cobble::{Config, DbBuilder, VolumeDescriptor, VolumeUsageKind};
use cobble_table::catalog::{
    Catalog, CatalogError, CatalogSchemaId, SchemaChange, TableIdentifier,
};
use cobble_table::file_catalog::{FileCatalog, FileCatalogConfig};
use cobble_table::{DataField, LogicalType, Table, TableSchema, Value};
use std::sync::Arc;

#[test]
fn file_catalog_namespace_and_table_lifecycle_survives_restart() {
    let root = tempfile::tempdir().unwrap();
    let storage_id = "catalog-lifecycle";
    let mut volume = VolumeDescriptor::new(
        format!("file://{}", root.path().display()),
        vec![
            VolumeUsageKind::PrimaryDataPriorityHigh,
            VolumeUsageKind::Meta,
        ],
    );
    volume.access_id = Some("runtime-access-id".to_string());
    volume.secret_key = Some("runtime-secret-key".to_string());
    let config = Config {
        volumes: vec![volume],
        total_buckets: 8,
        ..Config::default()
    };
    let open_config = FileCatalogConfig::new(storage_id);
    let analytics = vec!["production".to_string(), "analytics".to_string()];
    let scratch = vec!["scratch".to_string()];
    let accounts = TableIdentifier::new(analytics.clone(), "accounts");
    let events = TableIdentifier::new(analytics.clone(), "events");
    let schema = table_schema();
    let schema_one = schema_after_add();
    let schema_two = schema_after_changes();
    let shard_config = |name: &str| Config {
        volumes: VolumeDescriptor::single_volume(format!(
            "file://{}",
            root.path().join(name).display()
        )),
        total_buckets: 8,
        ..Config::default()
    };
    let shard_one = DbBuilder::new(shard_config("shard-one"))
        .bucket_ranges(vec![0..=7])
        .db_id("catalog-shard-one")
        .open()
        .unwrap();
    let shard_two = DbBuilder::new(shard_config("shard-two"))
        .bucket_ranges(vec![0..=7])
        .db_id("catalog-shard-two")
        .open()
        .unwrap();
    let mut unrelated = shard_two.update_schema();
    unrelated.ensure_column_family_exists("unrelated").unwrap();
    unrelated.commit();

    let (accounts_id, events_id) = {
        let catalog = Arc::new(FileCatalog::open(&config, open_config.clone()).unwrap());
        let second = Arc::new(FileCatalog::open(&config, open_config.clone()).unwrap());
        assert!(catalog.list_namespaces().unwrap().is_empty());
        catalog.create_namespace(analytics.clone()).unwrap();
        assert_eq!(second.list_namespaces().unwrap(), vec![analytics.clone()]);
        second.create_namespace(scratch.clone()).unwrap();
        assert_eq!(
            catalog.list_namespaces().unwrap(),
            vec![analytics.clone(), scratch.clone()]
        );
        assert!(matches!(
            catalog.create_namespace(analytics.clone()),
            Err(CatalogError::NamespaceAlreadyExists(_))
        ));
        assert!(matches!(
            catalog.create_table(TableIdentifier::new(["missing"], "orphan"), schema.clone()),
            Err(CatalogError::NamespaceNotFound(_))
        ));

        let accounts_table = catalog
            .create_table(accounts.clone(), schema.clone())
            .unwrap();
        assert!(second.table_exists(&accounts).unwrap());
        let events_table = second.create_table(events.clone(), schema.clone()).unwrap();
        assert_ne!(accounts_table.table_id(), events_table.table_id());
        assert_eq!(accounts_table.table_id().as_u32(), 1);
        assert_eq!(events_table.table_id().as_u32(), 2);
        assert_eq!(accounts_table.catalog_schema_id().as_u32(), 0);
        assert_eq!(events_table.catalog_schema_id().as_u32(), 0);
        assert!(matches!(
            catalog.create_table(accounts.clone(), schema.clone()),
            Err(CatalogError::TableAlreadyExists(_))
        ));
        assert!(matches!(
            catalog.drop_namespace(&analytics),
            Err(CatalogError::NamespaceNotEmpty(_))
        ));

        let row = vec![
            Value::String("tenant-a".to_string()),
            Value::Int64(7),
            Value::Binary(vec![1, 2, 3].into()),
        ];
        let table_one = catalog.materialize_table(&shard_one, &accounts).unwrap();
        let table_two = catalog.materialize_table(&shard_two, &accounts).unwrap();
        let key_one = build_key(&table_one, &row[..2]);
        let key_two = build_key(&table_two, &row[..2]);
        let deleted_row = vec![
            Value::String("tenant-a".to_string()),
            Value::Int64(8),
            Value::Binary(vec![8].into()),
        ];
        let deleted_key = build_key(&table_one, &deleted_row[..2]);
        table_one.put(&row).unwrap();
        table_one.put(&deleted_row).unwrap();
        table_one.delete(&deleted_key).unwrap();
        table_two.put(&row).unwrap();
        let mapping_one = catalog
            .load_shard_schema_mapping(&accounts, shard_one.id(), CatalogSchemaId::from(0))
            .unwrap();
        let mapping_two = catalog
            .load_shard_schema_mapping(&accounts, shard_two.id(), CatalogSchemaId::from(0))
            .unwrap();
        assert_ne!(mapping_one.core_schema_id(), mapping_two.core_schema_id());
        assert_eq!(mapping_one.db_id(), shard_one.id());
        assert_eq!(mapping_one.table_id(), accounts_table.table_id());
        assert_eq!(table_one.get(&key_one).unwrap(), Some(row.clone()));
        assert_eq!(table_two.get(&key_two).unwrap(), Some(row.clone()));

        let evolved = catalog
            .evolve_schema(
                &accounts,
                vec![SchemaChange::AddField(
                    DataField::new(4, "region", LogicalType::string().nullable()).unwrap(),
                )],
            )
            .unwrap();
        assert_eq!(evolved.catalog_schema_id().as_u32(), 1);
        assert_eq!(evolved.schema(), &schema_one);
        assert_eq!(second.load_table(&accounts).unwrap().schema(), &schema_one);
        assert_eq!(
            second
                .load_table_schema(&accounts, CatalogSchemaId::from(0))
                .unwrap(),
            schema
        );
        let table_one = catalog.materialize_table(&shard_one, &accounts).unwrap();
        let key_one = build_key(&table_one, &row[..2]);
        let mut expected = row.clone();
        expected.push(Value::Null);
        assert_eq!(table_one.get(&key_one).unwrap(), Some(expected));
        let deleted_key = build_key(&table_one, &deleted_row[..2]);
        assert_eq!(table_one.get(&deleted_key).unwrap(), None);
        assert_eq!(
            catalog
                .load_shard_schema_mapping(&accounts, shard_one.id(), CatalogSchemaId::from(1))
                .unwrap()
                .catalog_schema_id(),
            CatalogSchemaId::from(1)
        );
        let evolved = second
            .evolve_schema(
                &accounts,
                vec![
                    SchemaChange::AddField(
                        DataField::new(5, "state", LogicalType::int32().nullable()).unwrap(),
                    ),
                    SchemaChange::RenameField {
                        field_id: 5.into(),
                        new_name: "status".to_string(),
                    },
                    SchemaChange::RenameField {
                        field_id: 3.into(),
                        new_name: "body".to_string(),
                    },
                    SchemaChange::RenameField {
                        field_id: 4.into(),
                        new_name: "zone".to_string(),
                    },
                    SchemaChange::DropField(4.into()),
                ],
            )
            .unwrap();
        assert_eq!(evolved.catalog_schema_id().as_u32(), 2);
        assert_eq!(evolved.schema(), &schema_two);
        assert_eq!(
            catalog
                .load_table_schema(&accounts, CatalogSchemaId::from(1))
                .unwrap(),
            schema_one
        );
        let table_one = second.materialize_table(&shard_one, &accounts).unwrap();
        let key_one = build_key(&table_one, &row[..2]);
        let expected = vec![row[0].clone(), row[1].clone(), row[2].clone(), Value::Null];
        assert_eq!(table_one.get(&key_one).unwrap(), Some(expected));
        let deleted_key = build_key(&table_one, &deleted_row[..2]);
        assert_eq!(table_one.get(&deleted_key).unwrap(), None);
        let stable_mapping = second
            .load_shard_schema_mapping(&accounts, shard_one.id(), CatalogSchemaId::from(2))
            .unwrap()
            .core_schema_id();
        let mut unrelated = shard_one.update_schema();
        unrelated
            .ensure_column_family_exists("after-materialize")
            .unwrap();
        unrelated.commit();
        let applied_version = shard_one.current_schema().version();
        assert_eq!(
            second
                .materialize_table(&shard_one, &accounts)
                .unwrap()
                .get(&key_one)
                .unwrap(),
            Some(vec![
                row[0].clone(),
                row[1].clone(),
                row[2].clone(),
                Value::Null
            ])
        );
        assert_eq!(shard_one.current_schema().version(), applied_version);
        assert_eq!(
            second
                .load_shard_schema_mapping(&accounts, shard_one.id(), CatalogSchemaId::from(2))
                .unwrap()
                .core_schema_id(),
            stable_mapping
        );

        second.materialize_table(&shard_two, &accounts).unwrap();
        let pre_rollback_mapping = second
            .load_shard_schema_mapping(&accounts, shard_two.id(), CatalogSchemaId::from(2))
            .unwrap()
            .core_schema_id();
        let rolled_back_shard = DbBuilder::new(shard_config("shard-two-rollback"))
            .bucket_ranges(vec![0..=7])
            .db_id(shard_two.id())
            .open()
            .unwrap();
        second
            .materialize_table(&rolled_back_shard, &accounts)
            .unwrap();
        let repaired_mapping = second
            .load_shard_schema_mapping(&accounts, shard_two.id(), CatalogSchemaId::from(2))
            .unwrap()
            .core_schema_id();
        assert!(repaired_mapping < pre_rollback_mapping);

        let event_row = vec![
            Value::String("tenant-b".to_string()),
            Value::Int64(9),
            Value::Binary(vec![9].into()),
        ];
        let event_table = catalog.materialize_table(&shard_one, &events).unwrap();
        let deleted_event_row = vec![
            Value::String("tenant-b".to_string()),
            Value::Int64(10),
            Value::Binary(vec![10].into()),
        ];
        let deleted_event_key = build_key(&event_table, &deleted_event_row[..2]);
        event_table.put(&event_row).unwrap();
        event_table.put(&deleted_event_row).unwrap();
        event_table.delete(&deleted_event_key).unwrap();
        second
            .evolve_schema(&events, vec![SchemaChange::DropField(3.into())])
            .unwrap();
        let event_table = catalog.materialize_table(&shard_one, &events).unwrap();
        let event_key = build_key(&event_table, &event_row[..2]);
        assert_eq!(
            event_table.get(&event_key).unwrap(),
            Some(event_row[..2].to_vec())
        );
        let deleted_event_key = build_key(&event_table, &deleted_event_row[..2]);
        assert_eq!(event_table.get(&deleted_event_key).unwrap(), None);
        second
            .evolve_schema(
                &events,
                vec![SchemaChange::AddField(
                    DataField::new(6, "optional", LogicalType::int32().nullable()).unwrap(),
                )],
            )
            .unwrap();
        let event_table = catalog.materialize_table(&shard_one, &events).unwrap();
        let event_key = build_key(&event_table, &event_row[..2]);
        assert_eq!(
            event_table.get(&event_key).unwrap(),
            Some(vec![
                event_row[0].clone(),
                event_row[1].clone(),
                Value::Null
            ])
        );
        let deleted_event_key = build_key(&event_table, &deleted_event_row[..2]);
        assert_eq!(event_table.get(&deleted_event_key).unwrap(), None);
        assert!(matches!(
            catalog.evolve_schema(&accounts, vec![SchemaChange::DropField(1.into())]),
            Err(CatalogError::InvalidSchemaEvolution(_))
        ));
        assert!(matches!(
            catalog.evolve_schema(
                &accounts,
                vec![SchemaChange::AddField(
                    DataField::new(6, "required", LogicalType::string()).unwrap()
                )]
            ),
            Err(CatalogError::InvalidSchemaEvolution(_))
        ));
        assert!(matches!(
            catalog.evolve_schema(
                &accounts,
                vec![SchemaChange::AddField(
                    DataField::new(4, "reused", LogicalType::int32().nullable()).unwrap()
                )]
            ),
            Err(CatalogError::InvalidSchemaEvolution(_))
        ));
        assert!(matches!(
            catalog.rename_table(
                &TableIdentifier::new(analytics.clone(), "missing"),
                "events".to_string()
            ),
            Err(CatalogError::TableNotFound(_))
        ));

        let customers = second
            .rename_table(&accounts, "customers".to_string())
            .unwrap();
        assert_eq!(customers.table_id(), accounts_table.table_id());
        assert_eq!(customers.catalog_schema_id().as_u32(), 2);
        assert_eq!(customers.schema(), &schema_two);
        assert!(!catalog.table_exists(&accounts).unwrap());
        assert!(matches!(
            catalog.rename_table(&events, " customers ".to_string()),
            Err(CatalogError::InvalidIdentifier(_))
        ));
        (customers.table_id(), events_table.table_id())
    };

    {
        let catalog = FileCatalog::open(&config, open_config.clone()).unwrap();
        assert_eq!(
            catalog.list_namespaces().unwrap(),
            vec![analytics.clone(), scratch.clone()]
        );
        let customers = TableIdentifier::new(analytics.clone(), "customers");
        let loaded = catalog.load_table(&customers).unwrap();
        assert_eq!(loaded.table_id(), accounts_id);
        assert_eq!(loaded.catalog_schema_id().as_u32(), 2);
        assert_eq!(loaded.schema(), &schema_two);
        let schema_zero_mapping = catalog
            .load_shard_schema_mapping(&customers, shard_one.id(), CatalogSchemaId::from(0))
            .unwrap();
        let schema_two_mapping = catalog
            .load_shard_schema_mapping(&customers, shard_one.id(), CatalogSchemaId::from(2))
            .unwrap();
        assert_eq!(
            schema_zero_mapping.catalog_schema_id(),
            CatalogSchemaId::from(0)
        );
        assert!(schema_two_mapping.core_schema_id() > schema_zero_mapping.core_schema_id());
        assert_eq!(
            catalog
                .load_table_schema(&customers, CatalogSchemaId::from(0))
                .unwrap(),
            schema
        );
        assert_eq!(
            catalog
                .load_table_schema(&customers, CatalogSchemaId::from(1))
                .unwrap(),
            schema_one
        );
        assert!(matches!(
            catalog.load_table_schema(&customers, CatalogSchemaId::from(3)),
            Err(CatalogError::SchemaNotFound { .. })
        ));
        let tables = catalog.list_tables(&analytics).unwrap();
        assert_eq!(
            tables.iter().map(TableIdentifier::name).collect::<Vec<_>>(),
            vec!["customers", "events"]
        );
        assert_eq!(
            catalog.load_table(&tables[1]).unwrap().table_id(),
            events_id
        );
        assert_eq!(
            catalog
                .load_table(&tables[1])
                .unwrap()
                .catalog_schema_id()
                .as_u32(),
            2
        );
        assert!(matches!(
            catalog.load_table(&accounts),
            Err(CatalogError::TableNotFound(_))
        ));

        catalog.drop_table(&customers).unwrap();
        catalog.drop_table(&events).unwrap();
        assert!(matches!(
            catalog.drop_table(&events),
            Err(CatalogError::TableNotFound(_))
        ));
        catalog.drop_namespace(&analytics).unwrap();
        catalog.drop_namespace(&scratch).unwrap();
        assert!(catalog.list_namespaces().unwrap().is_empty());
    }

    let catalog = FileCatalog::open(&config, open_config).unwrap();
    assert!(catalog.list_namespaces().unwrap().is_empty());
    let metadata_root = root.path().join(storage_id).join("catalog");
    let files = all_files(&metadata_root);
    assert!(files.iter().any(|path| path.ends_with("CURRENT")));
    assert!(files.iter().any(|path| {
        path.file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with("CATALOG-"))
    }));
    for path in files {
        let bytes = std::fs::read(path).unwrap();
        for needle in [b"runtime-access-id".as_slice(), b"runtime-secret-key"] {
            assert!(!bytes.windows(needle.len()).any(|value| value == needle));
        }
    }
}

fn table_schema() -> TableSchema {
    TableSchema::new(
        vec![
            DataField::new(1, "tenant", LogicalType::string()).unwrap(),
            DataField::new(2, "id", LogicalType::int64()).unwrap(),
            DataField::new(3, "payload", LogicalType::binary().nullable()).unwrap(),
        ],
        vec![1.into(), 2.into()],
        vec![1.into()],
    )
    .unwrap()
}

fn schema_after_add() -> TableSchema {
    let mut fields = table_schema().fields;
    fields.push(DataField::new(4, "region", LogicalType::string().nullable()).unwrap());
    TableSchema::new(fields, vec![1.into(), 2.into()], vec![1.into()]).unwrap()
}

fn schema_after_changes() -> TableSchema {
    TableSchema::new(
        vec![
            DataField::new(1, "tenant", LogicalType::string()).unwrap(),
            DataField::new(2, "id", LogicalType::int64()).unwrap(),
            DataField::new(3, "body", LogicalType::binary().nullable()).unwrap(),
            DataField::new(5, "status", LogicalType::int32().nullable()).unwrap(),
        ],
        vec![1.into(), 2.into()],
        vec![1.into()],
    )
    .unwrap()
}

fn build_key(table: &Table<'_>, values: &[Value]) -> cobble_table::TableKey {
    let mut builder = table.key_builder();
    for value in values {
        builder.push(value.clone());
    }
    builder.build().unwrap()
}

fn all_files(root: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut pending = vec![root.to_path_buf()];
    let mut files = Vec::new();
    while let Some(path) = pending.pop() {
        for entry in std::fs::read_dir(path).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                pending.push(path);
            } else {
                files.push(path);
            }
        }
    }
    files
}
