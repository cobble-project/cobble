use super::*;
use crate::compaction::dedicated::DedicatedCompactionInput;
use crate::data_file::{DataFile, DataFileType};
use crate::file::TrackedFileId;
use crate::lsm::{LSMTreeVersion, Level};

/// Builds a `DedicatedDataFile` with the given path and key range (hex-encoded keys).
fn make_dedicated_file(file_id: u64, path: &str, start: &str, end: &str) -> DedicatedDataFile {
    DedicatedDataFile {
        file_id,
        file_type: "sst".to_string(),
        path: path.to_string(),
        schema_id: 1,
        size: 100,
        start_key: start.to_string(),
        end_key: end.to_string(),
        has_separated_values: false,
        bucket_range_start: 0,
        bucket_range_end: 0,
        effective_bucket_range_start: 0,
        effective_bucket_range_end: 0,
        vlog_file_seq_offset: 0,
        max_expired_at: 0,
    }
}

/// Builds a live `DataFile` Arc for LSM construction.
fn make_live_file(file_manager: &Arc<FileManager>, file_id: u64) -> Arc<DataFile> {
    Arc::new(
        DataFile::new(
            DataFileType::SSTable,
            vec![0u8],
            vec![255u8],
            file_id,
            TrackedFileId::new(file_manager, file_id),
            1,
            100,
            0u16..=0u16,
            0u16..=0u16,
        )
        .with_separated_values(false),
    )
}

/// Builds an `LSMTreeVersion` with the given files in the specified level.
fn make_tree_version(level: u8, files: Vec<Arc<DataFile>>) -> LSMTreeVersion {
    LSMTreeVersion {
        levels: vec![Level {
            ordinal: level,
            tiered: false,
            files,
        }],
    }
}

/// When all inputs are gone, classify_rewrite must return AppliedInMemory (not a
/// "Committed" state), regardless of whether outputs are in the LSM. This ensures
/// commit_and_verify always runs - the in-memory LSM alone cannot prove manifest durability.
#[test]
fn test_classify_rewrite_inputs_gone_is_applied_in_memory() {
    use crate::config::{Config, VolumeDescriptor};
    use crate::metrics_manager::MetricsManager;
    let dir = tempfile::tempdir_in("/tmp").unwrap();
    let base = format!("file://{}", dir.path().display());
    let config = Config {
        volumes: VolumeDescriptor::single_volume(base.clone()),
        ..Config::default()
    };
    let metrics = Arc::new(MetricsManager::new("test"));
    let fm = Arc::new(FileManager::from_config(&config, "test-db", metrics).unwrap());

    // Create real files on disk so registration succeeds.
    let (input_id, _) = fm.create_data_file_with_prefix("data").unwrap();
    let (output_id, _) = fm.create_data_file_with_prefix("data").unwrap();
    let input_path = fm.get_data_file_full_path(input_id).unwrap();
    let output_path = fm.get_data_file_full_path(output_id).unwrap();

    let input_file = make_dedicated_file(input_id, &input_path, "00", "ff");
    let output_file = make_dedicated_file(output_id, &output_path, "00", "ff");
    let inputs = vec![DedicatedCompactionInput {
        level: 0,
        file: input_file,
    }];
    let outputs = vec![output_file];

    // Case 1: inputs gone, outputs present in LSM -> AppliedInMemory (not Committed).
    let live_output = make_live_file(&fm, output_id);
    let tree_version = make_tree_version(1, vec![live_output]);
    let status = classify_rewrite(&tree_version, &inputs, &outputs, 1, &fm);
    assert_eq!(status, OperationStatus::AppliedInMemory);

    // Case 2: inputs gone, outputs NOT in LSM -> still AppliedInMemory (not Conflict).
    let empty_tree = LSMTreeVersion { levels: vec![] };
    let status = classify_rewrite(&empty_tree, &inputs, &outputs, 1, &fm);
    assert_eq!(status, OperationStatus::AppliedInMemory);
}

/// When inputs are present and no outputs exist, classify_rewrite must return Pending.
#[test]
fn test_classify_rewrite_inputs_present_is_pending() {
    use crate::config::{Config, VolumeDescriptor};
    use crate::metrics_manager::MetricsManager;
    let dir = tempfile::tempdir_in("/tmp").unwrap();
    let base = format!("file://{}", dir.path().display());
    let config = Config {
        volumes: VolumeDescriptor::single_volume(base.clone()),
        ..Config::default()
    };
    let metrics = Arc::new(MetricsManager::new("test"));
    let fm = Arc::new(FileManager::from_config(&config, "test-db", metrics).unwrap());

    // Create a real file so registration and path resolution work.
    let (input_id, _) = fm.create_data_file_with_prefix("data").unwrap();
    let (output_id, _) = fm.create_data_file_with_prefix("data").unwrap();
    let input_path = fm.get_data_file_full_path(input_id).unwrap();
    let output_path = fm.get_data_file_full_path(output_id).unwrap();

    let input_file = make_dedicated_file(input_id, &input_path, "00", "ff");
    let live_input = make_live_file(&fm, input_id);
    let inputs = vec![DedicatedCompactionInput {
        level: 0,
        file: input_file,
    }];
    let outputs = vec![make_dedicated_file(output_id, &output_path, "00", "ff")];
    let tree_version = make_tree_version(0, vec![live_input]);
    let status = classify_rewrite(&tree_version, &inputs, &outputs, 1, &fm);
    assert_eq!(status, OperationStatus::Pending);

    let rerouted_input = DedicatedCompactionInput {
        level: 0,
        file: make_dedicated_file(input_id, "file:///alternate-volume/input.sst", "00", "ff"),
    };
    let status = classify_rewrite(&tree_version, &[rerouted_input], &outputs, 1, &fm);
    assert_eq!(
        status,
        OperationStatus::Pending,
        "input replica routing must not change logical compaction identity"
    );
}

/// classify_apply_error must map storage errors to PreserveAndRetry (not Terminal),
/// so the poller preserves files instead of cleaning them up.
#[test]
fn test_classify_apply_error_storage_is_preserve() {
    let io_err = Error::IoError("disk unavailable".to_string());
    assert!(matches!(
        classify_apply_error(io_err),
        ApplyError::PreserveAndRetry(_)
    ));

    let fs_err = Error::FileSystemError("s3 down".to_string());
    assert!(matches!(
        classify_apply_error(fs_err),
        ApplyError::PreserveAndRetry(_)
    ));
}

/// classify_apply_error must map structural errors to Terminal.
#[test]
fn test_classify_apply_error_structural_is_terminal() {
    let state_err = Error::InvalidState("bad hex".to_string());
    assert!(matches!(
        classify_apply_error(state_err),
        ApplyError::Terminal(_)
    ));
}

/// build_path_to_id_from_lsm must return an error (not silently succeed) when an output
/// is not found in the LSM. The caller maps this to PreserveAndRetry, ensuring files are
/// preserved rather than cleaned up.
#[test]
fn test_build_path_to_id_from_lsm_missing_output_errors() {
    use crate::config::{Config, VolumeDescriptor};
    use crate::metrics_manager::MetricsManager;
    let dir = tempfile::tempdir_in("/tmp").unwrap();
    let config = Config {
        volumes: VolumeDescriptor::single_volume(format!("file://{}", dir.path().display())),
        ..Config::default()
    };
    let metrics = Arc::new(MetricsManager::new("test"));
    let fm = Arc::new(FileManager::from_config(&config, "test-db", metrics).unwrap());

    let output = make_dedicated_file(2, "file:///tmp/test/data/b.sst", "00", "ff");
    let empty_tree = LSMTreeVersion { levels: vec![] };
    let result = build_path_to_id_from_lsm(&empty_tree, &[output], &fm, "job-1");
    assert!(result.is_err(), "missing output must produce an error");
    // The error must NOT be a transient I/O error - it's a structural InvalidState that
    // the caller maps to PreserveAndRetry.
    let err = result.unwrap_err();
    assert!(!is_transient_error(&err));
}

#[test]
fn snapshot_tree_lookup_uses_scope_instead_of_current_index() {
    let first_scope = LSMTreeScope::new(0u16..=1u16, 0);
    let shifted_scope = LSMTreeScope::new(2u16..=3u16, 0);
    let manifest = crate::snapshot::manifest::ManifestSnapshot {
        version: crate::snapshot::manifest::MANIFEST_VERSION_CURRENT,
        id: 7,
        timestamp_seconds: 0,
        seq_id: 11,
        topology_epoch: 0,
        wal_checkpoint_id: 0,
        wal_volume: None,
        latest_schema_id: 0,
        data_size_bytes: 0,
        incremental_data_size_bytes: 0,
        bucket_ranges: vec![0u16..=3u16],
        lsm_tree_bucket_ranges: vec![0u16..=1u16, 2u16..=3u16],
        tree_scopes: vec![first_scope, shifted_scope.clone()],
        tree_levels: vec![
            vec![crate::manifest_model::ManifestLevel {
                ordinal: 1,
                tiered: false,
                files: Vec::new(),
            }],
            vec![crate::manifest_model::ManifestLevel {
                ordinal: 7,
                tiered: false,
                files: Vec::new(),
            }],
        ],
        vlog_files: Vec::new(),
        active_memtable_data: Vec::new(),
        truncation_cursors: Vec::new(),
    };

    let levels = manifest_tree_levels_by_scope(&manifest, &shifted_scope)
        .unwrap()
        .unwrap();
    assert_eq!(levels[0].ordinal, 7);
    assert!(
        manifest_tree_levels_by_scope(&manifest, &LSMTreeScope::new(4u16..=5u16, 0))
            .unwrap()
            .is_none()
    );
}

#[test]
fn absence_only_operations_require_a_new_snapshot_proof() {
    let input = DedicatedCompactionInput {
        level: 0,
        file: make_dedicated_file(1, "file:///tmp/input.sst", "00", "ff"),
    };
    assert!(!operation_has_positive_manifest_evidence(
        &DedicatedCompactionOperation::Drop {
            inputs: vec![input.clone()],
        }
    ));
    assert!(!operation_has_positive_manifest_evidence(
        &DedicatedCompactionOperation::Rewrite {
            inputs: vec![input.clone()],
            output_level: 1,
            outputs: Vec::new(),
        }
    ));
    assert!(operation_has_positive_manifest_evidence(
        &DedicatedCompactionOperation::Rewrite {
            inputs: vec![input.clone()],
            output_level: 1,
            outputs: vec![make_dedicated_file(2, "file:///tmp/output.sst", "00", "ff",)],
        }
    ));
    assert!(operation_has_positive_manifest_evidence(
        &DedicatedCompactionOperation::TrivialMove {
            input,
            output_level: 1,
        }
    ));
}
