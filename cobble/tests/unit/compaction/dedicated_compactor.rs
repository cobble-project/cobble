use super::*;
use crate::config::{CompactionMode, VolumeDescriptor, VolumeUsageKind};
use crate::file::{File, SequentialWriteFile};

#[test]
fn dedicated_compactor_uses_writer_property_volumes() {
    let meta_dir = tempfile::tempdir().unwrap();
    let writer_data_dir = tempfile::tempdir().unwrap();
    let process_data_dir = tempfile::tempdir().unwrap();
    let db_id = "dedicated-properties-volume";
    let writer_config = Config {
        volumes: vec![
            VolumeDescriptor::new(
                format!("file://{}", meta_dir.path().display()),
                vec![VolumeUsageKind::Meta],
            ),
            VolumeDescriptor::new(
                format!("file://{}", writer_data_dir.path().display()),
                vec![VolumeUsageKind::PrimaryDataPriorityHigh],
            ),
        ],
        compaction_mode: CompactionMode::Dedicated,
        ..Config::default()
    };
    let metrics = Arc::new(MetricsManager::new(db_id));
    let writer_file_manager = FileManager::from_config(&writer_config, db_id, metrics).unwrap();
    crate::properties::persist_db_properties(&writer_file_manager, db_id, &writer_config).unwrap();

    let process_config = Config {
        volumes: vec![
            VolumeDescriptor::new(
                format!("file://{}", meta_dir.path().display()),
                vec![VolumeUsageKind::Meta],
            ),
            VolumeDescriptor::new(
                format!("file://{}", process_data_dir.path().display()),
                vec![VolumeUsageKind::PrimaryDataPriorityHigh],
            ),
        ],
        compaction_mode: CompactionMode::Dedicated,
        ..Config::default()
    };
    let compactor = DedicatedCompactor::open(process_config, db_id).unwrap();
    let (file_id, mut writer) = compactor
        .file_manager
        .create_data_file_with_prefix("compaction/jobs/test/data")
        .unwrap();
    writer.write(b"property-selected").unwrap();
    writer.close().unwrap();
    let output_path = compactor
        .file_manager
        .get_data_file_full_path(file_id)
        .unwrap();

    assert!(output_path.starts_with(&format!("file://{}", writer_data_dir.path().display())));
    assert!(!output_path.starts_with(&format!("file://{}", process_data_dir.path().display())));
}

#[test]
fn plan_volume_paths_are_absolute_and_credential_free() {
    let sanitized = sanitized_absolute_volume_path(
        "s3://access-id:secret@bucket/state/db-1?endpoint=http%3A%2F%2Flocalhost%3A9000#ignored",
    )
    .unwrap();
    let url = url::Url::parse(&sanitized).unwrap();

    assert_eq!(url.as_str(), "s3://bucket/state/db-1");
    assert!(url.username().is_empty());
    assert!(url.password().is_none());
    assert!(url.query().is_none());
    assert!(url.fragment().is_none());

    let local = sanitized_absolute_volume_path("relative/cache").unwrap();
    assert!(local.starts_with("file:///"));
}

#[test]
fn executor_applies_its_own_storage_options_to_plan_path() {
    let applied = apply_plan_volume_path(
        "s3://executor-id:executor-secret@bucket/config-root?endpoint=storage.example",
        "s3://bucket/state/db-1",
    )
    .unwrap();
    let url = url::Url::parse(&applied).unwrap();

    assert_eq!(url.path(), "/state/db-1");
    assert_eq!(url.username(), "executor-id");
    assert_eq!(url.password(), Some("executor-secret"));
    assert_eq!(url.query(), Some("endpoint=storage.example"));
}

#[test]
fn executor_rejects_credentials_in_plan_path() {
    let error = apply_plan_volume_path(
        "s3://bucket/config-root",
        "s3://attacker:secret@bucket/state/db-1?endpoint=other",
    )
    .unwrap_err();

    assert!(error.to_string().contains("must not contain credentials"));
}
