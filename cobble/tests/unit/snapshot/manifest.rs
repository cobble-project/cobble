use super::*;
use crate::data_file::{DataFile, DataFileType};

#[test]
fn decode_manifest_requires_version() {
    let without_version = br#"{
            "id": 1,
            "seq_id": 2,
            "latest_schema_id": 3,
            "bucket_ranges": [],
            "lsm_tree_bucket_ranges": [],
            "tree_levels": [],
            "vlog_files": [],
            "active_memtable_data": []
        }"#;
    let err = match decode_manifest(without_version) {
        Ok(_) => panic!("expected missing version to be rejected"),
        Err(err) => err,
    };
    assert!(err.to_string().contains("Failed to decode manifest"));
}

#[test]
fn decode_manifest_rejects_future_version() {
    let future = format!(
        r#"{{
                "version": {},
                "id": 1,
                "seq_id": 2,
                "latest_schema_id": 3,
                "data_size_bytes": 0,
                "incremental_data_size_bytes": 0,
                "bucket_ranges": [],
                "lsm_tree_bucket_ranges": [],
                "tree_scopes": [],
                "tree_levels": [],
                "vlog_files": [],
                "active_memtable_data": []
            }}"#,
        MANIFEST_VERSION_CURRENT + 1
    );
    let err = match decode_manifest(future.as_bytes()) {
        Ok(_) => panic!("expected future manifest version to be rejected"),
        Err(err) => err,
    };
    assert!(
        err.to_string()
            .contains("Unsupported snapshot manifest version")
    );
}

#[test]
fn decode_manifest_v2_backward_compatible_defaults_max_expired_at() {
    // A version-2 manifest with a file that has no max_expired_at field.
    // serde(default) should fill in 0 (no expiration).
    let v2 = r#"{
            "version": 2,
            "id": 1,
            "seq_id": 2,
            "latest_schema_id": 3,
            "data_size_bytes": 0,
            "incremental_data_size_bytes": 0,
            "bucket_ranges": [],
            "lsm_tree_bucket_ranges": [],
            "tree_scopes": [],
            "tree_levels": [[{"ordinal": 1, "tiered": false, "files": [
                {"file_id": 10, "file_type": "sst", "schema_id": 1, "size": 100,
                 "start_key": "61", "end_key": "7a", "path": "data/10.sst",
                 "has_separated_values": false,
                 "bucket_range_start": 0, "bucket_range_end": 0,
                 "effective_bucket_range_start": 0, "effective_bucket_range_end": 0,
                 "vlog_file_seq_offset": 0}
            ]}]],
            "vlog_files": [],
            "active_memtable_data": []
        }"#;
    let payload = decode_manifest(v2.as_bytes()).expect("v2 manifest should decode");
    match payload {
        ManifestPayload::Snapshot(s) => {
            let file = &s.tree_levels[0][0].files[0];
            assert_eq!(file.max_expired_at, 0);
            assert_eq!(s.timestamp_seconds, 0);
        }
        _ => panic!("expected snapshot payload"),
    }
}

#[test]
fn decode_manifest_v3_preserves_file_extensions() {
    let v3 = r#"{
            "version": 3,
            "id": 1,
            "seq_id": 2,
            "topology_epoch": 7,
            "latest_schema_id": 3,
            "data_size_bytes": 0,
            "incremental_data_size_bytes": 0,
            "bucket_ranges": [],
            "lsm_tree_bucket_ranges": [],
            "tree_scopes": [],
            "tree_levels": [[{"ordinal": 1, "tiered": false, "files": [
                {"file_id": 10, "file_type": "sst", "schema_id": 1, "size": 100,
                 "start_key": "61", "end_key": "7a", "path": "data/10.sst",
                 "has_separated_values": false,
                 "bucket_range_start": 0, "bucket_range_end": 0,
                 "effective_bucket_range_start": 0, "effective_bucket_range_end": 0,
                 "vlog_file_seq_offset": 0,
                 "max_expired_at": 5000,
                 "origin": {"kind": "external_leased", "export_id": "runtime-export"}}
            ]}]],
            "vlog_files": [],
            "active_memtable_data": []
        }"#;
    let payload = decode_manifest(v3.as_bytes()).expect("v3 manifest should decode");
    match payload {
        ManifestPayload::Snapshot(s) => {
            let file = &s.tree_levels[0][0].files[0];
            assert_eq!(file.max_expired_at, 5000);
            assert_eq!(s.topology_epoch, 7);
            assert!(matches!(
                file.origin,
                crate::file::logical_file::ReplicaOrigin::ExternalLeased { ref export_id }
                    if export_id == "runtime-export"
            ));
        }
        _ => panic!("expected snapshot payload"),
    }
}

#[test]
fn topology_epoch_change_forces_full_snapshot_manifest() {
    let file = Arc::new(DataFile::new_untracked(
        DataFileType::SSTable,
        b"a".to_vec(),
        b"b".to_vec(),
        1,
        0,
        1,
        0..=0,
        0..=0,
    ));
    let mut base = DbSnapshot::new(1, "snapshot/1", None);
    base.lsm_versions = vec![LSMTreeVersion {
        levels: vec![Level {
            ordinal: 0,
            tiered: true,
            files: Vec::new(),
        }],
    }];
    let mut next = base.clone();
    next.id = 2;
    next.topology_epoch = 1;
    next.lsm_versions[0].levels[0].files.push(file);

    assert!(build_incremental_tree_level_edits(&base, &next).is_none());
}

#[test]
fn decode_manifest_rejects_previous_physical_key_format() {
    let previous = r#"{
            "version": 1,
            "id": 1,
            "seq_id": 2,
            "latest_schema_id": 3,
            "data_size_bytes": 0,
            "incremental_data_size_bytes": 0,
            "bucket_ranges": [],
            "lsm_tree_bucket_ranges": [],
            "tree_scopes": [],
            "tree_levels": [],
            "vlog_files": [],
            "active_memtable_data": []
        }"#;
    let err = match decode_manifest(previous.as_bytes()) {
        Ok(_) => panic!("version 1 must be rejected"),
        Err(err) => err,
    };
    assert!(err.to_string().contains(&format!(
        "Unsupported snapshot manifest version: 1 (expected 2..={MANIFEST_VERSION_CURRENT})"
    )));
}
