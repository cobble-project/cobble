use super::{runtime_volumes, shared_volume};
use cobble::{VolumeDescriptor, VolumeUsageKind::*};

#[test]
fn catalog_and_runtime_keep_only_their_storage_roles() {
    let primary = [
        PrimaryDataPriorityHigh,
        PrimaryDataPriorityMedium,
        PrimaryDataPriorityLow,
    ];
    let mut source = VolumeDescriptor::new(
        "s3://bucket/root",
        vec![Meta, Snapshot, Wal, Cache, Readonly],
    );
    for kind in primary {
        source.set_usage(kind);
    }
    source.access_id = Some("access".into());
    source.secret_key = Some("secret".into());
    let prefix = "warehouse/tables/TABLE-7";
    let shared = shared_volume(&source, prefix).unwrap();
    assert_eq!(shared.base_dir, "s3://bucket/root/warehouse/tables/TABLE-7");
    for kind in [Meta, Snapshot, Wal] {
        assert!(shared.supports(kind));
    }
    for kind in [Cache, Readonly].into_iter().chain(primary) {
        assert!(!shared.supports(kind));
    }
    assert!(shared_volume(&VolumeDescriptor::new("/primary", primary.to_vec()), prefix).is_none());

    let runtime = runtime_volumes(&source, prefix);
    assert_eq!(runtime.len(), 2);
    assert_eq!(runtime[0].base_dir, shared.base_dir);
    for kind in primary.into_iter().chain([Cache]) {
        assert!(runtime[0].supports(kind));
    }
    for kind in [Meta, Snapshot, Wal, Readonly] {
        assert!(!runtime[0].supports(kind));
    }
    assert_eq!(runtime[1].base_dir, source.base_dir);
    assert_eq!(
        runtime[1].kinds,
        VolumeDescriptor::new("", vec![Readonly]).kinds
    );
    for volume in [&shared, &runtime[0], &runtime[1]] {
        assert_eq!(volume.access_id, source.access_id);
        assert_eq!(volume.secret_key, source.secret_key);
    }
    assert!(
        runtime_volumes(
            &VolumeDescriptor::new("/meta", vec![Meta, Snapshot, Wal]),
            prefix
        )
        .is_empty()
    );
}
