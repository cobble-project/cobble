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
