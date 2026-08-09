use super::*;
use crate::metrics_manager::MetricsManager;
use crate::{Config, VolumeDescriptor};
use tokio::time::{Duration, timeout};

fn snapshot_with_base(id: u64, base_snapshot_id: Option<u64>) -> Arc<DbSnapshot> {
    let mut snapshot = DbSnapshot::new(id, &format!("SNAPSHOT-{id}"), None);
    snapshot.base_snapshot_id = base_snapshot_id;
    Arc::new(snapshot)
}

#[test]
fn suggested_base_fallback_skips_cancelled_ancestors() {
    let grandparent = snapshot_with_base(1, None);
    let parent = snapshot_with_base(2, Some(1));
    assert!(parent.try_cancel());
    let child = snapshot_with_base(3, Some(2));

    let snapshots = BTreeMap::from([
        (1, Arc::clone(&grandparent)),
        (2, Arc::clone(&parent)),
        (3, Arc::clone(&child)),
    ]);

    assert_eq!(suggested_base_fallback_id(&snapshots, 3), Some(1));
}

#[test]
fn suggested_base_fallback_clears_on_broken_chain() {
    let parent = snapshot_with_base(2, Some(99));
    assert!(parent.try_cancel());
    let child = snapshot_with_base(3, Some(2));

    let snapshots = BTreeMap::from([(2, Arc::clone(&parent)), (3, Arc::clone(&child))]);

    assert_eq!(suggested_base_fallback_id(&snapshots, 3), None);
}

#[test]
fn snapshot_copy_permits_bound_concurrent_transfers() {
    let root = "/tmp/snapshot_copy_transfer_budget";
    let _ = std::fs::remove_dir_all(root);
    let file_manager = FileManager::from_config(
        &Config {
            file_transfer_concurrency: 2,
            volumes: VolumeDescriptor::single_volume(format!("file://{root}")),
            ..Config::default()
        },
        "snapshot-copy-transfer-budget",
        Arc::new(MetricsManager::new("snapshot-copy-transfer-budget")),
    )
    .unwrap();
    let runtime = Runtime::new().unwrap();
    runtime.block_on(async {
        let first = acquire_snapshot_transfer_permit(&file_manager)
            .await
            .unwrap();
        let second = acquire_snapshot_transfer_permit(&file_manager)
            .await
            .unwrap();

        assert!(
            timeout(
                Duration::from_millis(20),
                acquire_snapshot_transfer_permit(&file_manager)
            )
            .await
            .is_err()
        );

        drop(first);
        assert!(
            timeout(
                Duration::from_secs(1),
                acquire_snapshot_transfer_permit(&file_manager)
            )
            .await
            .unwrap()
            .is_ok()
        );
        drop(second);
    });
    let _ = std::fs::remove_dir_all(root);
}
