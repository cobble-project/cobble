use super::*;

#[test]
fn test_result_round_trip() {
    let result = DedicatedCompactionResult {
        version: DEDICATED_COMPACTION_RESULT_VERSION,
        job_id: "test-job-123".to_string(),
        source: DedicatedCompactionSource::Runtime {
            generation: 42,
            seq_id: 99,
        },
        topology_epoch: 0,
        lsm_tree_idx: 0,
        tree_scope: LSMTreeScope::new(0u16..=0u16, 0),
        operation: DedicatedCompactionOperation::Rewrite {
            inputs: vec![DedicatedCompactionInput {
                level: 0,
                file: DedicatedDataFile {
                    file_id: 1,
                    file_type: "SSTable".to_string(),
                    path: "data/abc.sst".to_string(),
                    schema_id: 1,
                    size: 100,
                    start_key: "00".to_string(),
                    end_key: "ff".to_string(),
                    has_separated_values: false,
                    bucket_range_start: 0,
                    bucket_range_end: 0,
                    effective_bucket_range_start: 0,
                    effective_bucket_range_end: 0,
                    vlog_file_seq_offset: 0,
                    max_expired_at: 0,
                },
            }],
            output_level: 1,
            outputs: vec![],
        },
        vlog_entry_deltas: vec![(5, -3)],
        created_at_ms: 1234567890,
    };
    let bytes = result.encode().unwrap();
    let decoded = DedicatedCompactionResult::decode(&bytes).unwrap();
    assert_eq!(decoded.job_id, result.job_id);
    assert_eq!(decoded.source, result.source);
    assert_eq!(decoded.operation, result.operation);
    assert_eq!(decoded.vlog_entry_deltas, result.vlog_entry_deltas);
}

#[test]
fn test_result_version_rejected() {
    let bytes = serde_json::json!({
        "version": 999,
        "job_id": "x",
        "source": { "kind": "runtime", "generation": 0, "seq_id": 0 },
        "lsm_tree_idx": 0,
        "tree_scope": { "bucket_range": [0, 0], "column_family_id": 0 },
        "operation": { "Drop": { "inputs": [] } },
        "vlog_entry_deltas": [],
        "created_at_ms": 0,
    })
    .to_string()
    .into_bytes();
    assert!(DedicatedCompactionResult::decode(&bytes).is_err());
}

#[test]
fn test_parse_job_id() {
    assert_eq!(
        parse_dedicated_compaction_job_id("COMPACTION-abc-123"),
        Some("abc-123".to_string())
    );
    assert_eq!(parse_dedicated_compaction_job_id("COMPACTION-"), None);
    assert_eq!(parse_dedicated_compaction_job_id("SNAPSHOT-5"), None);
    assert_eq!(
        parse_dedicated_compaction_job_id("compaction/results/COMPACTION-xyz"),
        Some("xyz".to_string())
    );
}

#[test]
fn test_operation_helpers() {
    let input = DedicatedCompactionInput {
        level: 0,
        file: DedicatedDataFile {
            file_id: 1,
            file_type: "SSTable".to_string(),
            path: "data/a.sst".to_string(),
            schema_id: 1,
            size: 10,
            start_key: "00".to_string(),
            end_key: "ff".to_string(),
            has_separated_values: false,
            bucket_range_start: 0,
            bucket_range_end: 0,
            effective_bucket_range_start: 0,
            effective_bucket_range_end: 0,
            vlog_file_seq_offset: 0,
            max_expired_at: 0,
        },
    };
    let rewrite = DedicatedCompactionOperation::Rewrite {
        inputs: vec![input.clone()],
        output_level: 1,
        outputs: Vec::new(),
    };
    assert_eq!(rewrite.inputs().len(), 1);
    assert_eq!(rewrite.output_level(), Some(1));
    assert!(rewrite.outputs().is_empty());

    let mv = DedicatedCompactionOperation::TrivialMove {
        input: input.clone(),
        output_level: 2,
    };
    assert_eq!(mv.inputs().len(), 1);
    assert_eq!(mv.output_level(), Some(2));
    assert!(mv.outputs().is_empty());

    let drop_op = DedicatedCompactionOperation::Drop {
        inputs: vec![input],
    };
    assert_eq!(drop_op.inputs().len(), 1);
    assert_eq!(drop_op.output_level(), None);
    assert!(drop_op.outputs().is_empty());
}

/// Builds a FileManager backed by **separate** metadata and data volumes, each under a
/// unique tempfile directory. The metadata volume holds leases/manifests; the data volume
/// holds compaction output files. This mirrors a multi-volume production setup and verifies
/// the sweep correctly checks the lease on the metadata volume while deleting output files
/// on the data volume.
fn build_fm_multi_volume(db_id: &str) -> (Arc<FileManager>, tempfile::TempDir, tempfile::TempDir) {
    use crate::config::{Config, VolumeDescriptor, VolumeUsageKind};
    use crate::metrics_manager::MetricsManager;
    let meta_dir = tempfile::tempdir_in("/tmp").expect("create meta tempdir");
    let data_dir = tempfile::tempdir_in("/tmp").expect("create data tempdir");
    let config = Config {
        volumes: vec![
            VolumeDescriptor::new(
                format!("file://{}", meta_dir.path().display()),
                vec![VolumeUsageKind::Meta, VolumeUsageKind::Snapshot],
            ),
            VolumeDescriptor::new(
                format!("file://{}", data_dir.path().display()),
                vec![VolumeUsageKind::PrimaryDataPriorityHigh],
            ),
        ],
        ..Config::default()
    };
    let metrics = std::sync::Arc::new(MetricsManager::new("sweep-test"));
    let fm = Arc::new(FileManager::from_config(&config, db_id, metrics).unwrap());
    (fm, meta_dir, data_dir)
}

/// The orphan sweep must not delete:
/// - Job directories with a fresh lease (active compactor).
/// - Job directories whose outputs are referenced by the manifest (committed).
///
/// And must delete:
/// - Job directories with an expired lease, no result, and no manifest reference (crashed).
///
/// This test uses separate metadata and data volumes to verify the lease (on the metadata
/// volume) is correctly checked while output files (on the data volume) are correctly
/// matched against manifest paths and deleted.
#[test]
fn test_orphan_sweep_preserves_active_and_committed() {
    let db_id = "sweep-db";
    let (fm, _meta_dir, _data_dir) = build_fm_multi_volume(db_id);

    // --- Active job: fresh lease, no result, not referenced. Should survive. ---
    let active_job = "job-active";
    write_job_lease(&fm, active_job).unwrap();
    assert!(
        has_active_dedicated_compaction(&fm).unwrap(),
        "a lease alone must fence topology changes before the first output exists"
    );
    // Create a dummy output file under the job's data dir on the data volume.
    fm.create_data_file_with_prefix(&format!(
        "{}/{}/data",
        DEDICATED_COMPACTION_JOBS_DIR, active_job
    ))
    .unwrap();

    // This job crashes before creating an output directory. Its metadata-only lease must
    // still be swept once stale, otherwise it would permanently fence topology changes.
    let lease_only_job = "job-lease-only";
    write_job_lease(&fm, lease_only_job).unwrap();

    // --- Crashed job: stale lease (written then we sleep past min_age), no result,
    //     not referenced. Should be deleted. ---
    let crashed_job = "job-crashed";
    write_job_lease(&fm, crashed_job).unwrap();
    fm.create_data_file_with_prefix(&format!(
        "{}/{}/data",
        DEDICATED_COMPACTION_JOBS_DIR, crashed_job
    ))
    .unwrap();

    // --- Committed job: stale lease, no result, but output is referenced by manifest.
    //     Should survive. ---
    let committed_job = "job-committed";
    write_job_lease(&fm, committed_job).unwrap();
    let (committed_file_id, _writer) = fm
        .create_data_file_with_prefix(&format!(
            "{}/{}/data",
            DEDICATED_COMPACTION_JOBS_DIR, committed_job
        ))
        .unwrap();
    // Build the manifest path set that includes the committed job's output.
    let committed_path = fm
        .get_data_file_full_path(committed_file_id)
        .expect("committed file path");
    let mut manifest_paths = std::collections::HashSet::new();
    manifest_paths.insert(committed_path);

    // The filesystem's `last_modified` returns timestamps in seconds, so the sweep
    // compares age in seconds. Sleep 2 seconds so the crashed and committed leases are
    // stale (>= 1s old with min_age_ms=100 -> min_age_secs=1), then refresh the active
    // job's lease so it's fresh (< 1s old).
    std::thread::sleep(std::time::Duration::from_secs(2));
    write_job_lease(&fm, active_job).unwrap();

    // Sweep with a 100ms min age (rounds up to 1s). The active job's lease is fresh,
    // so it survives. The crashed and committed jobs have stale leases. The committed
    // job survives because its output is referenced by the manifest. The crashed data job
    // and the metadata-only lease are swept.
    let swept = sweep_orphan_job_dirs(&fm, &manifest_paths, 100).unwrap();

    assert_eq!(swept, 2, "both crashed job forms should be swept");

    // Active job dir should still exist (lease is fresh).
    let active_dir = format!("{}/{}", DEDICATED_COMPACTION_JOBS_DIR, active_job);
    assert!(
        fm.data_volume_path_exists(&active_dir).unwrap(),
        "active job directory should survive (fresh lease)"
    );

    remove_job_lease(&fm, committed_job).unwrap();
    assert!(
        !fm.metadata_file_exists_untracked(&format!(
            "{}/{}/{}",
            DEDICATED_COMPACTION_JOBS_DIR, committed_job, DEDICATED_COMPACTION_LEASE_FILE
        ))
        .unwrap()
    );
    let committed_dir = format!("{}/{}", DEDICATED_COMPACTION_JOBS_DIR, committed_job);
    assert!(
        fm.data_volume_path_exists(&committed_dir).unwrap(),
        "removing a completed lease must retain manifest-referenced output"
    );

    // Committed job dir should still exist (output referenced by manifest).
    assert!(
        fm.data_volume_path_exists(&committed_dir).unwrap(),
        "committed job directory should survive (referenced by manifest)"
    );

    // Crashed job dir should be gone.
    let crashed_dir = format!("{}/{}", DEDICATED_COMPACTION_JOBS_DIR, crashed_job);
    assert!(
        !fm.data_volume_path_exists(&crashed_dir).unwrap(),
        "crashed job directory should be swept"
    );
    assert!(
        !fm.metadata_file_exists_untracked(&format!(
            "{}/{}/{}",
            DEDICATED_COMPACTION_JOBS_DIR, lease_only_job, DEDICATED_COMPACTION_LEASE_FILE
        ))
        .unwrap(),
        "metadata-only crashed lease should be swept"
    );
}

#[test]
fn test_dedicated_data_file_preserves_max_expired_at() {
    let manifest_file = ManifestFile {
        file_id: 10,
        file_type: "sst".to_string(),
        schema_id: 1,
        size: 100,
        start_key: "61".to_string(),
        end_key: "7a".to_string(),
        path: "data/10.sst".to_string(),
        has_separated_values: false,
        bucket_range_start: 0,
        bucket_range_end: 0,
        effective_bucket_range_start: 0,
        effective_bucket_range_end: 0,
        vlog_file_seq_offset: 0,
        max_expired_at: 5000,
        origin: crate::file::logical_file::ReplicaOrigin::Owned,
    };
    let dedicated: DedicatedDataFile = DedicatedDataFile::from(&manifest_file);
    assert_eq!(dedicated.max_expired_at, 5000);
    // Round-trip through JSON should preserve the value.
    let json = serde_json::to_string(&dedicated).unwrap();
    let decoded: DedicatedDataFile = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded.max_expired_at, 5000);
    // Fingerprint match should require max_expired_at equality.
    assert!(dedicated.matches_manifest_file(&manifest_file));
    let mut mismatched = manifest_file.clone();
    mismatched.max_expired_at = 0;
    assert!(!dedicated.matches_manifest_file(&mismatched));
}

#[test]
fn test_dedicated_data_file_backward_compatible_without_max_expired_at() {
    // JSON without max_expired_at (old format) should decode with default 0.
    let json = r#"{
            "file_id": 10,
            "file_type": "sst",
            "path": "data/10.sst",
            "schema_id": 1,
            "size": 100,
            "start_key": "61",
            "end_key": "7a",
            "has_separated_values": false,
            "bucket_range_start": 0,
            "bucket_range_end": 0,
            "effective_bucket_range_start": 0,
            "effective_bucket_range_end": 0,
            "vlog_file_seq_offset": 0
        }"#;
    let decoded: DedicatedDataFile = serde_json::from_str(json).unwrap();
    assert_eq!(decoded.max_expired_at, 0);
    assert_eq!(decoded.file_id, 10);
}
