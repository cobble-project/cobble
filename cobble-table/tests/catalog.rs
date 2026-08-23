use cobble::{Config, VolumeDescriptor, VolumeUsageKind};
use cobble_table::{
    Catalog, CatalogError, DataField, FileCatalog, FileCatalogConfig, LogicalType, TableIdentifier,
    TableSchema,
};
use std::sync::Arc;

#[test]
fn file_catalog_namespace_and_table_lifecycle_survives_restart() {
    let root = tempfile::tempdir().unwrap();
    let storage_id = "catalog-lifecycle";
    let mut volume = VolumeDescriptor::new(
        format!("file://{}", root.path().display()),
        vec![VolumeUsageKind::Meta],
    );
    volume.access_id = Some("runtime-access-id".to_string());
    volume.secret_key = Some("runtime-secret-key".to_string());
    let config = Config {
        volumes: vec![volume],
        ..Config::default()
    };
    let open_config = FileCatalogConfig::new(storage_id);
    let analytics = vec!["production".to_string(), "analytics".to_string()];
    let scratch = vec!["scratch".to_string()];
    let accounts = TableIdentifier::new(analytics.clone(), "accounts");
    let events = TableIdentifier::new(analytics.clone(), "events");
    let schema = table_schema();

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
        assert!(matches!(
            catalog.create_table(accounts.clone(), schema.clone()),
            Err(CatalogError::TableAlreadyExists(_))
        ));
        assert!(matches!(
            catalog.drop_namespace(&analytics),
            Err(CatalogError::NamespaceNotEmpty(_))
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
        assert_eq!(customers.schema(), &schema);
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
        assert_eq!(loaded.schema(), &schema);
        let tables = catalog.list_tables(&analytics).unwrap();
        assert_eq!(
            tables.iter().map(TableIdentifier::name).collect::<Vec<_>>(),
            vec!["customers", "events"]
        );
        assert_eq!(
            catalog.load_table(&tables[1]).unwrap().table_id(),
            events_id
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
