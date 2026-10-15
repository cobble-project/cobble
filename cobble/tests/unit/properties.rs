use super::*;
use crate::config::{VolumeDescriptor, VolumeUsageKind};
use crate::file::FileManager;
use crate::file::{FileSystem, FileSystemRegistry};
use crate::metrics_manager::MetricsManager;
use size::Size;
use std::sync::Arc;

#[test]
fn properties_rewrite_without_atomic_rename_stays_flat() {
    let dir = tempfile::tempdir().unwrap();
    let root = format!("file://{}", dir.path().display());
    let inner = FileSystemRegistry::new().get_or_register(&root).unwrap();
    let fs = Arc::new(crate::file::atomic_replace::tests::TestFileSystem::new(
        inner,
    ));
    let manager = FileManager::with_defaults(
        fs.clone(),
        Arc::new(MetricsManager::new("non-atomic-properties")),
    )
    .unwrap();
    let mut config = Config {
        volumes: VolumeDescriptor::single_volume(root),
        ..Config::default()
    };
    persist_db_properties(&manager, "shard", &config).unwrap();
    config.l0_file_limit += 1;
    assert!(refresh_db_properties(&manager, "shard", &config).unwrap());
    assert_eq!(
        load_db_properties(&manager, "shard")
            .unwrap()
            .config
            .l0_file_limit,
        config.l0_file_limit
    );
    assert!(!refresh_db_properties(&manager, "shard", &config).unwrap());
    assert!(fs.exists(DB_PROPERTIES_NAME).unwrap());
    assert!(!fs.exists(&format!("{DB_PROPERTIES_NAME}-ATOMIC/")).unwrap());
    *fs.fail_rename_to.lock().unwrap() = Some(DB_PROPERTIES_NAME.to_string());
    config.l0_file_limit += 1;
    assert!(refresh_db_properties(&manager, "shard", &config).is_err());
    assert!(!fs.exists(DB_PROPERTIES_NAME).unwrap());
    assert!(matches!(
        load_db_properties(&manager, "shard"),
        Err(Error::IoError(_))
    ));
    *fs.fail_rename_to.lock().unwrap() = None;
    assert!(refresh_db_properties(&manager, "shard", &config).unwrap());
    assert_eq!(
        load_db_properties(&manager, "shard")
            .unwrap()
            .config
            .l0_file_limit,
        config.l0_file_limit
    );
}

#[test]
fn properties_are_plain_toml_without_volume_credentials() {
    let dir = tempfile::tempdir().unwrap();
    let root = format!("file://{}", dir.path().display());
    let mut volume = VolumeDescriptor::new(
        "s3://writer:embedded-secret@example-bucket/data?access_key_id=query-ak",
        vec![
            VolumeUsageKind::Meta,
            VolumeUsageKind::PrimaryDataPriorityMedium,
            VolumeUsageKind::Wal,
        ],
    );
    volume.access_id = Some("field-ak".to_string());
    volume.secret_key = Some("field-sk".to_string());
    volume.size_limit = Some(Size::from_mib(16));
    volume.custom_options = Some(HashMap::from([
        ("region".to_string(), "test-region".to_string()),
        ("secret_access_key".to_string(), "option-sk".to_string()),
    ]));
    let mut oss_volume = VolumeDescriptor::new(
        "oss://example-bucket/data?access_key_secret=oss-query-sk",
        vec![VolumeUsageKind::Snapshot],
    );
    oss_volume.custom_options = Some(HashMap::from([
        ("endpoint".to_string(), "https://oss.example".to_string()),
        ("access_key_id".to_string(), "oss-option-ak".to_string()),
        ("access_key_secret".to_string(), "oss-option-sk".to_string()),
    ]));
    let mut cos_volume = VolumeDescriptor::new(
        "cos://example-bucket/data?secret_id=cos-query-id&security_token=cos-query-token",
        vec![VolumeUsageKind::Snapshot],
    );
    cos_volume.custom_options = Some(HashMap::from([
        ("secret_id".to_string(), "cos-option-id".to_string()),
        ("security_token".to_string(), "cos-option-token".to_string()),
    ]));
    let mut ftp_volume = VolumeDescriptor::new(
        "ftp://example.com/data?user=ftp-query-user&password=ftp-query-password",
        vec![VolumeUsageKind::Snapshot],
    );
    ftp_volume.custom_options = Some(HashMap::from([
        ("user".to_string(), "ftp-option-user".to_string()),
        ("password".to_string(), "ftp-option-password".to_string()),
    ]));
    let config = Config {
        volumes: vec![volume, oss_volume, cos_volume, ftp_volume],
        l0_file_limit: 17,
        wal_enabled: true,
        ..Config::default()
    };
    let storage_config = Config {
        volumes: VolumeDescriptor::single_volume(root),
        ..Config::default()
    };
    let metrics = Arc::new(MetricsManager::new("properties-test"));
    let file_manager = FileManager::from_config(&storage_config, "shard-7", metrics).unwrap();

    persist_db_properties(&file_manager, "shard-7", &config).unwrap();
    let reader = file_manager
        .open_metadata_file_reader_untracked(DB_PROPERTIES_NAME)
        .unwrap();
    let contents = String::from_utf8(reader.read_all().unwrap().to_vec()).unwrap();
    let decoded: DbProperties = toml::from_str(&contents).unwrap();
    let route = config.volumes[0].without_credentials();
    assert_eq!(
        serde_json::to_value(&route).unwrap(),
        serde_json::to_value(&decoded.config.volumes[0]).unwrap()
    );
    let rebound = route.with_credentials_from(&config);
    assert_eq!(rebound.base_dir, config.volumes[0].base_dir);
    assert_eq!(rebound.access_id, config.volumes[0].access_id);
    assert_eq!(rebound.secret_key, config.volumes[0].secret_key);
    assert_eq!(rebound.custom_options, config.volumes[0].custom_options);
    let oss_route = decoded.config.volumes[1].with_credentials_from(&config);
    assert_eq!(oss_route.base_dir, config.volumes[1].base_dir);
    assert_eq!(oss_route.custom_options, config.volumes[1].custom_options);
    for index in [2, 3] {
        let rebound = decoded.config.volumes[index].with_credentials_from(&config);
        assert_eq!(rebound.base_dir, config.volumes[index].base_dir);
        assert_eq!(rebound.custom_options, config.volumes[index].custom_options);
    }

    assert_eq!(decoded.version, DB_PROPERTIES_VERSION_CURRENT);
    assert_eq!(decoded.db_id, "shard-7");
    assert_eq!(decoded.config.l0_file_limit, 17);
    assert_eq!(
        decoded.config.volumes[0].size_limit,
        Some(Size::from_mib(16))
    );
    assert!(decoded.config.volumes[0].supports(VolumeUsageKind::PrimaryDataPriorityMedium));
    assert!(decoded.config.volumes[0].supports(VolumeUsageKind::Wal));
    assert!(decoded.config.wal_enabled);
    assert_eq!(
        decoded.config.volumes[0]
            .custom_options
            .as_ref()
            .unwrap()
            .get("region")
            .map(String::as_str),
        Some("test-region")
    );
    for secret in [
        "writer",
        "embedded-secret",
        "query-ak",
        "field-ak",
        "field-sk",
        "option-sk",
        "oss-query-sk",
        "oss-option-ak",
        "oss-option-sk",
        "cos-query-id",
        "cos-query-token",
        "cos-option-id",
        "cos-option-token",
        "ftp-query-user",
        "ftp-query-password",
        "ftp-option-user",
        "ftp-option-password",
    ] {
        assert!(!contents.contains(secret), "PROPERTIES leaked {secret}");
    }
}

#[test]
fn compactor_uses_persisted_volume_order_and_process_credentials() {
    let dir = tempfile::tempdir().unwrap();
    let metadata_root = format!("file://{}", dir.path().display());
    let property_config = Config {
        volumes: vec![
            VolumeDescriptor::new(
                "s3://bucket/slow",
                vec![VolumeUsageKind::PrimaryDataPriorityLow],
            ),
            VolumeDescriptor::new(
                "s3://bucket/fast",
                vec![VolumeUsageKind::PrimaryDataPriorityHigh],
            ),
        ],
        ..Config::default()
    };
    let storage_config = Config {
        volumes: VolumeDescriptor::single_volume(metadata_root),
        ..Config::default()
    };
    let metrics = Arc::new(MetricsManager::new("properties-resolve-test"));
    let file_manager = FileManager::from_config(&storage_config, "shard-9", metrics).unwrap();
    persist_db_properties(&file_manager, "shard-9", &property_config).unwrap();

    let mut fast_runtime = VolumeDescriptor::new(
        "s3://bucket/fast",
        vec![VolumeUsageKind::PrimaryDataPriorityLow],
    );
    fast_runtime.access_id = Some("runtime-ak".to_string());
    fast_runtime.secret_key = Some("runtime-sk".to_string());
    let process_config = Config {
        volumes: vec![fast_runtime],
        ..Config::default()
    };
    let resolved = load_compactor_config(&file_manager, "shard-9", &process_config).unwrap();

    assert_eq!(resolved.volumes.len(), 2);
    assert_eq!(resolved.volumes[0].base_dir, "s3://bucket/slow");
    assert_eq!(resolved.volumes[1].base_dir, "s3://bucket/fast");
    assert_eq!(resolved.volumes[1].access_id.as_deref(), Some("runtime-ak"));
    assert_eq!(
        resolved.volumes[1].secret_key.as_deref(),
        Some("runtime-sk")
    );
    assert!(resolved.volumes[1].supports(VolumeUsageKind::PrimaryDataPriorityHigh));
}

#[test]
fn startup_refreshes_properties_only_when_sanitized_config_changes() {
    let dir = tempfile::tempdir().unwrap();
    let metadata_root = format!("file://{}", dir.path().display());
    let storage_config = Config {
        volumes: VolumeDescriptor::single_volume(metadata_root),
        ..Config::default()
    };
    let metrics = Arc::new(MetricsManager::new("properties-refresh-test"));
    let file_manager = FileManager::from_config(&storage_config, "shard-11", metrics).unwrap();
    let mut writer_config = Config {
        volumes: VolumeDescriptor::single_volume("s3://bucket/data"),
        l0_file_limit: 7,
        ..Config::default()
    };
    writer_config.volumes[0].access_id = Some("first-ak".to_string());
    writer_config.volumes[0].secret_key = Some("first-sk".to_string());

    assert!(refresh_db_properties(&file_manager, "shard-11", &writer_config).unwrap());
    assert!(!refresh_db_properties(&file_manager, "shard-11", &writer_config).unwrap());

    writer_config.volumes[0].access_id = Some("rotated-ak".to_string());
    writer_config.volumes[0].secret_key = Some("rotated-sk".to_string());
    assert!(!refresh_db_properties(&file_manager, "shard-11", &writer_config).unwrap());

    writer_config.l0_file_limit = 13;
    assert!(refresh_db_properties(&file_manager, "shard-11", &writer_config).unwrap());
    let refreshed = load_db_properties(&file_manager, "shard-11").unwrap();
    assert_eq!(refreshed.config.l0_file_limit, 13);
}

#[test]
fn startup_replaces_malformed_properties() {
    let dir = tempfile::tempdir().unwrap();
    let metadata_root = format!("file://{}", dir.path().display());
    let storage_config = Config {
        volumes: VolumeDescriptor::single_volume(metadata_root),
        ..Config::default()
    };
    let metrics = Arc::new(MetricsManager::new("properties-malformed-test"));
    let file_manager = FileManager::from_config(&storage_config, "shard-12", metrics).unwrap();
    assert!(matches!(
        load_db_properties(&file_manager, "shard-12"),
        Err(Error::IoError(_))
    ));
    file_manager
        .rewrite_plain_metadata_file(DB_PROPERTIES_NAME, b"not = [valid")
        .unwrap();
    assert!(matches!(
        load_db_properties(&file_manager, "shard-12"),
        Err(Error::IoError(_))
    ));
    file_manager
        .rewrite_plain_metadata_file(DB_PROPERTIES_NAME, &[0xff])
        .unwrap();
    assert!(matches!(
        load_db_properties(&file_manager, "shard-12"),
        Err(Error::IoError(_))
    ));
    let writer_config = Config {
        volumes: VolumeDescriptor::single_volume("s3://bucket/data"),
        ..Config::default()
    };

    assert!(refresh_db_properties(&file_manager, "shard-12", &writer_config).unwrap());
    load_db_properties(&file_manager, "shard-12").unwrap();
    let mut invalid = DbProperties::from_writer("shard-12", &writer_config);
    invalid.version += 1;
    file_manager
        .rewrite_plain_metadata_file(
            DB_PROPERTIES_NAME,
            serialize_db_properties(&invalid).unwrap().as_bytes(),
        )
        .unwrap();
    assert!(matches!(
        load_db_properties(&file_manager, "shard-12"),
        Err(Error::InvalidState(_))
    ));
    invalid.version = DB_PROPERTIES_VERSION_CURRENT;
    invalid.db_id = "another-shard".to_string();
    file_manager
        .rewrite_plain_metadata_file(
            DB_PROPERTIES_NAME,
            serialize_db_properties(&invalid).unwrap().as_bytes(),
        )
        .unwrap();
    assert!(matches!(
        load_db_properties(&file_manager, "shard-12"),
        Err(Error::InvalidState(_))
    ));
    invalid.db_id = "shard-12".to_string();
    invalid.config.volumes.clear();
    file_manager
        .rewrite_plain_metadata_file(
            DB_PROPERTIES_NAME,
            serialize_db_properties(&invalid).unwrap().as_bytes(),
        )
        .unwrap();
    assert!(matches!(
        load_db_properties(&file_manager, "shard-12"),
        Err(Error::InvalidState(_))
    ));
}
