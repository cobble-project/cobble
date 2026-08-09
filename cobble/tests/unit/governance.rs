use super::*;
use crate::file::FileSystemRegistry;

fn make_manager(root: &str) -> GovernanceManager {
    let registry = FileSystemRegistry::new();
    let fs = registry
        .get_or_register(format!("file://{}", root))
        .unwrap();
    GovernanceManager::with_file_lock(fs, root).unwrap()
}

fn cleanup_root(path: &str) {
    let _ = std::fs::remove_dir_all(path);
}

#[test]
fn test_governance_insert_and_publish_round_trip() {
    let root = "/tmp/governance_insert_publish";
    cleanup_root(root);
    let manager = make_manager(root);
    let id = "db-a".to_string();
    manager
        .insert_and_publish(&id, vec![0u16..=4u16, 10u16..=11u16], 12)
        .unwrap();
    cleanup_root(root);
}

#[test]
fn test_governance_insert_rejects_overlap() {
    let root = "/tmp/governance_overlap";
    cleanup_root(root);
    let manager = make_manager(root);
    let id_a = "db-a".to_string();
    let id_b = "db-b".to_string();
    manager
        .insert_and_publish(&id_a, vec![0u16..=4u16], 10)
        .unwrap();
    let err = manager
        .insert_and_publish(&id_b, vec![4u16..=5u16], 10)
        .unwrap_err();
    assert!(matches!(err, Error::IoError(_)));
    cleanup_root(root);
}

#[test]
fn test_governance_insert_rejects_total_bucket_mismatch() {
    let root = "/tmp/governance_total_bucket_mismatch";
    cleanup_root(root);
    let manager = make_manager(root);
    let id = "db-a".to_string();
    manager
        .insert_and_publish(&id, vec![0u16..=4u16], 10)
        .unwrap();
    let err = manager
        .insert_and_publish(&id, vec![0u16..=4u16], 12)
        .unwrap_err();
    assert!(matches!(err, Error::IoError(_)));
    cleanup_root(root);
}
