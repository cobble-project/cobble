use super::*;
use crate::config::{VolumeDescriptor, VolumeUsageKind};
use crate::file::FileManager;
use crate::metrics_manager::MetricsManager;
use size::Size;
use std::sync::Arc;

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
    let config = Config {
        volumes: vec![volume],
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
    let path = dir.path().join("shard-7").join(DB_PROPERTIES_NAME);
    let contents = std::fs::read_to_string(path).unwrap();
    let decoded: DbProperties = toml::from_str(&contents).unwrap();

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
    file_manager
        .write_plain_metadata_file_atomic(DB_PROPERTIES_NAME, b"not = [valid")
        .unwrap();
    let writer_config = Config {
        volumes: VolumeDescriptor::single_volume("s3://bucket/data"),
        ..Config::default()
    };

    assert!(refresh_db_properties(&file_manager, "shard-12", &writer_config).unwrap());
    load_db_properties(&file_manager, "shard-12").unwrap();
}
