use super::*;

#[test]
fn shard_mapping_reports_keep_the_lowest_core_schema_in_memory() {
    let root = tempfile::tempdir().unwrap();
    let config = Config {
        volumes: cobble::VolumeDescriptor::single_volume(format!(
            "file://{}",
            root.path().display()
        )),
        ..Config::default()
    };
    let store = Arc::new(FileCatalogSchemaStore {
        store: CatalogStore::open(&config, "mapping-memory").unwrap(),
        schema_mappings: Mutex::new(HashMap::new()),
    });
    let shared = Arc::clone(&store);
    let table_id = TableId::new(1);
    let schema_id = CatalogSchemaId::from(2);
    for core_schema_id in [9, 9, 12, 3, 8] {
        shared
            .record_shard_schema_mapping(ShardSchemaMapping::new(
                table_id,
                "shard",
                schema_id,
                core_schema_id,
            ))
            .unwrap();
    }
    shared
        .record_shard_schema_mapping(ShardSchemaMapping::new(
            table_id,
            "other-shard",
            schema_id,
            7,
        ))
        .unwrap();
    shared
        .record_shard_schema_mapping(ShardSchemaMapping::new(
            table_id,
            "shard",
            CatalogSchemaId::from(3),
            10,
        ))
        .unwrap();
    let mappings = lock(&store.schema_mappings).unwrap();
    assert_eq!(mappings.len(), 3);
    assert_eq!(mappings[&(table_id, "shard".to_string(), schema_id)], 3);
    assert_eq!(
        mappings[&(table_id, "other-shard".to_string(), schema_id)],
        7
    );
    drop(mappings);
    assert!(!root.path().join("mapping-memory/catalog/tables").exists());
    let weak = Arc::downgrade(&store);
    drop(store);
    assert!(weak.upgrade().is_some());
    drop(shared);
    assert!(weak.upgrade().is_none());
}
