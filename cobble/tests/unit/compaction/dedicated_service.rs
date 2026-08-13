use super::*;
use crate::config::{CompactionMode, VolumeDescriptor};

#[test]
fn discovery_accepts_parent_and_direct_paths_without_duplicates() {
    let dir = tempfile::tempdir().unwrap();
    let db_a = dir.path().join("db-a");
    let db_b = dir.path().join("db-b");
    std::fs::create_dir_all(db_a.join("runtime")).unwrap();
    std::fs::create_dir_all(db_b.join("snapshot")).unwrap();
    std::fs::write(db_a.join("runtime/CURRENT"), b"1\n").unwrap();
    std::fs::write(db_b.join("snapshot/SNAPSHOT-1"), b"manifest").unwrap();

    let mut config = Config {
        compaction_mode: CompactionMode::Dedicated,
        ..Config::default()
    };
    config.volumes = VolumeDescriptor::single_volume(dir.path().to_string_lossy().into_owned());
    let shards = discover_shards(
        &config,
        &[
            CompactionScanPath::Local(dir.path().to_path_buf()),
            CompactionScanPath::Local(db_a.clone()),
        ],
        &FileSystemRegistry::new(),
    );
    assert_eq!(shards.len(), 2);
    assert_eq!(
        shards
            .iter()
            .map(|shard| shard.db_id.as_str())
            .collect::<Vec<_>>(),
        vec!["db-a", "db-b"]
    );
    assert_eq!(
        shards[0].config.volumes[0].base_dir,
        dir.path().to_string_lossy()
    );
}

#[test]
fn exact_discovery_does_not_scan_siblings_or_children() {
    let dir = tempfile::tempdir().unwrap();
    let selected = dir.path().join("selected");
    let sibling = dir.path().join("sibling");
    let nested = selected.join("nested");
    for path in [&selected, &sibling, &nested] {
        std::fs::create_dir_all(path).unwrap();
        std::fs::write(path.join(DB_PROPERTIES_NAME), b"properties").unwrap();
    }

    let config = Config {
        volumes: VolumeDescriptor::single_volume(dir.path().to_string_lossy().into_owned()),
        compaction_mode: CompactionMode::Dedicated,
        ..Config::default()
    };
    let shards = discover_exact_shards(
        &config,
        &[CompactionScanPath::Local(selected)],
        &FileSystemRegistry::new(),
    );

    assert_eq!(shards.len(), 1);
    assert_eq!(shards[0].db_id, "selected");
}

#[test]
fn local_discovery_finds_nested_databases_and_rebinds_shared_volume() {
    let dir = tempfile::tempdir().unwrap();
    let shared = dir.path().join("shared");
    let db_dir = shared.join("tenant-a/db-7");
    std::fs::create_dir_all(&db_dir).unwrap();
    std::fs::write(db_dir.join(DB_PROPERTIES_NAME), b"properties").unwrap();

    let config = Config {
        volumes: vec![
            VolumeDescriptor::new(
                shared.to_string_lossy().into_owned(),
                vec![
                    VolumeUsageKind::Meta,
                    VolumeUsageKind::PrimaryDataPriorityHigh,
                ],
            ),
            VolumeDescriptor::new(
                dir.path().join("cache").to_string_lossy().into_owned(),
                vec![VolumeUsageKind::Cache],
            ),
        ],
        ..Config::default()
    };

    let shards = discover_shards(
        &config,
        &[CompactionScanPath::Local(shared)],
        &FileSystemRegistry::new(),
    );

    assert_eq!(shards.len(), 1);
    assert_eq!(shards[0].db_id, "db-7");
    assert_eq!(
        shards[0].config.volumes[0].base_dir,
        db_dir.parent().unwrap().to_string_lossy()
    );
    assert_eq!(
        shards[0].config.volumes[1].base_dir,
        dir.path()
            .join("cache/tenant-a")
            .to_string_lossy()
            .into_owned()
    );
    assert!(shards[0].config.volumes[1].supports(VolumeUsageKind::Cache));
}

#[test]
fn discovery_from_checkpoint_parent_reaches_flink_shared_state_database() {
    let dir = tempfile::tempdir().unwrap();
    let db_dir = dir
        .path()
        .join("0123456789abcdef/shared/op_state/data/db-7");
    std::fs::create_dir_all(&db_dir).unwrap();
    std::fs::write(db_dir.join(DB_PROPERTIES_NAME), b"properties").unwrap();
    let config = Config {
        volumes: VolumeDescriptor::single_volume(dir.path().to_string_lossy().into_owned()),
        compaction_mode: CompactionMode::Dedicated,
        ..Config::default()
    };

    let shards = discover_shards(
        &config,
        &[CompactionScanPath::Local(dir.path().to_path_buf())],
        &FileSystemRegistry::new(),
    );

    assert_eq!(shards.len(), 1);
    assert_eq!(shards[0].db_id, "db-7");
    assert_eq!(
        shards[0].config.volumes[0].base_dir,
        db_dir.parent().unwrap().to_string_lossy()
    );
}

#[test]
fn direct_single_volume_path_rebinds_volume_parent() {
    let dir = tempfile::tempdir().unwrap();
    let db_dir = dir.path().join("shard-7");
    std::fs::create_dir_all(&db_dir).unwrap();
    let config = Config {
        volumes: VolumeDescriptor::single_volume("/different/root"),
        ..Config::default()
    };
    let (resolved, db_id) = config_for_db_directory(&config, &db_dir).unwrap();
    assert_eq!(db_id, "shard-7");
    assert_eq!(resolved.volumes[0].base_dir, dir.path().to_string_lossy());
}

#[test]
fn storage_discovery_rebinds_the_metadata_volume_to_the_shard_parent() {
    let config = Config {
        volumes: vec![
            VolumeDescriptor::new("s3://bucket/cobble", vec![VolumeUsageKind::Meta]),
            VolumeDescriptor::new(
                "s3://bucket/data/shared",
                vec![VolumeUsageKind::PrimaryDataPriorityHigh],
            ),
            VolumeDescriptor::new("/var/cache/cobble", vec![VolumeUsageKind::Cache]),
        ],
        ..Config::default()
    };
    let base = normalized_url("s3://bucket/cobble").unwrap();

    let shard = storage_discovered_shard(&config, &base, "tenant-a/db-7").unwrap();

    assert_eq!(shard.db_id, "db-7");
    assert_eq!(
        shard.config.volumes[0].base_dir,
        "s3://bucket/cobble/tenant-a"
    );
    assert_eq!(
        shard.config.volumes[1].base_dir,
        "s3://bucket/data/shared/tenant-a"
    );
    assert_eq!(
        shard.config.volumes[2].base_dir,
        "/var/cache/cobble/tenant-a"
    );
    assert_eq!(
        shard.location,
        ShardLocation::Storage("s3://bucket/cobble/tenant-a/db-7".to_string())
    );
}

#[test]
fn storage_path_must_be_below_the_same_volume_authority() {
    let base = normalized_url("s3://bucket/cobble").unwrap();
    let child = normalized_url("s3://bucket/cobble/tenant-a").unwrap();
    let other = normalized_url("s3://other/cobble/tenant-a").unwrap();

    assert_eq!(
        relative_url_path(&base, &child).as_deref(),
        Some("tenant-a")
    );
    assert_eq!(relative_url_path(&base, &other), None);
}

#[test]
fn scan_path_parser_distinguishes_file_urls_and_storage_urls() {
    assert!(matches!(
        parse_scan_path("file:///tmp/cobble".to_string()).unwrap(),
        CompactionScanPath::Local(path) if path == Path::new("/tmp/cobble")
    ));
    assert!(matches!(
        parse_scan_path("s3://bucket/cobble".to_string()).unwrap(),
        CompactionScanPath::Storage(path) if path == "s3://bucket/cobble"
    ));
    assert!(matches!(
        parse_scan_path("/tmp/cobble".to_string()).unwrap(),
        CompactionScanPath::Local(path) if path == Path::new("/tmp/cobble")
    ));
    assert!(parse_scan_path(" ".to_string()).is_err());
}
