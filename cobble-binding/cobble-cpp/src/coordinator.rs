use cobble_binding::{Config, CoordinatorConfig, DbCoordinator, ShardSnapshotInput};

use crate::{
    BridgeResult,
    error::{format_cobble_error, input_error},
    ffi,
    snapshot::{shard_snapshot_input, snapshot},
};

pub(crate) struct NativeCoordinator {
    coordinator: DbCoordinator,
}

fn open(config: Config) -> BridgeResult<Box<NativeCoordinator>> {
    opendal::install_default();
    DbCoordinator::open(CoordinatorConfig::from_config(&config))
        .map(|coordinator| Box::new(NativeCoordinator { coordinator }))
        .map_err(format_cobble_error)
}

pub(crate) fn native_coordinator_open(config_json: &str) -> BridgeResult<Box<NativeCoordinator>> {
    open(Config::from_json_str(config_json).map_err(format_cobble_error)?)
}

pub(crate) fn native_coordinator_open_file(
    config_path: &str,
) -> BridgeResult<Box<NativeCoordinator>> {
    open(Config::from_path(config_path).map_err(format_cobble_error)?)
}

fn validate_coverage(total_buckets: u32, shards: &[ShardSnapshotInput]) -> BridgeResult<()> {
    if total_buckets == 0 || total_buckets > u32::from(u16::MAX) + 1 {
        return Err(input_error("total_buckets must be in range 1..=65536"));
    }
    if shards.is_empty() {
        return Err(input_error("shard snapshots must not be empty"));
    }
    let mut covered = vec![false; total_buckets as usize];
    for shard in shards {
        for range in &shard.ranges {
            if u32::from(*range.end()) >= total_buckets {
                return Err(input_error("shard range exceeds total_buckets"));
            }
            for bucket in range.clone() {
                let slot = &mut covered[bucket as usize];
                if *slot {
                    return Err(input_error("shard ranges overlap"));
                }
                *slot = true;
            }
        }
    }
    if covered.iter().any(|covered| !covered) {
        return Err(input_error(
            "shard ranges must cover every bucket exactly once",
        ));
    }
    Ok(())
}

pub(crate) fn native_coordinator_materialize_global_snapshot(
    coordinator: &NativeCoordinator,
    total_buckets: u32,
    snapshot_id: u64,
    shards: Vec<ffi::NativeShardSnapshot>,
) -> BridgeResult<ffi::NativeSnapshot> {
    let shards = shards
        .into_iter()
        .map(shard_snapshot_input)
        .collect::<BridgeResult<Vec<_>>>()?;
    validate_coverage(total_buckets, &shards)?;
    let global = coordinator
        .coordinator
        .take_global_snapshot_with_id(total_buckets, shards, snapshot_id)
        .map_err(format_cobble_error)?;
    coordinator
        .coordinator
        .materialize_global_snapshot(&global)
        .map_err(format_cobble_error)?;
    Ok(snapshot(global))
}

pub(crate) fn native_coordinator_get_global_snapshot(
    coordinator: &NativeCoordinator,
    snapshot_id: u64,
) -> BridgeResult<ffi::NativeSnapshot> {
    coordinator
        .coordinator
        .load_global_snapshot(snapshot_id)
        .map(snapshot)
        .map_err(format_cobble_error)
}

pub(crate) fn native_coordinator_list_global_snapshots(
    coordinator: &NativeCoordinator,
) -> BridgeResult<Vec<ffi::NativeSnapshot>> {
    coordinator
        .coordinator
        .list_global_snapshots()
        .map(|values| values.into_iter().map(snapshot).collect())
        .map_err(format_cobble_error)
}

pub(crate) fn native_coordinator_load_current_global_snapshot(
    coordinator: &NativeCoordinator,
) -> BridgeResult<Vec<ffi::NativeSnapshot>> {
    coordinator
        .coordinator
        .load_current_global_snapshot()
        .map(|value| value.into_iter().map(snapshot).collect())
        .map_err(format_cobble_error)
}

pub(crate) fn native_coordinator_retain_snapshot(
    coordinator: &NativeCoordinator,
    snapshot_id: u64,
) -> bool {
    coordinator.coordinator.retain_snapshot(snapshot_id)
}

pub(crate) fn native_coordinator_expire_snapshot(
    coordinator: &NativeCoordinator,
    snapshot_id: u64,
) -> BridgeResult<bool> {
    coordinator
        .coordinator
        .expire_snapshot(snapshot_id)
        .map_err(format_cobble_error)
}
