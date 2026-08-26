use std::sync::mpsc;

use crate::{
    BridgeResult,
    database::NativeDatabase,
    error::{format_cobble_error, input_error},
    ffi,
};

pub(crate) struct NativePendingSnapshot {
    id: u64,
    receiver: Option<mpsc::Receiver<BridgeResult<cobble_binding::GlobalSnapshotManifest>>>,
}

fn family((name, id): (String, u8)) -> ffi::NativeFamily {
    ffi::NativeFamily { name, id }
}

fn snapshot(value: cobble_binding::GlobalSnapshotManifest) -> ffi::NativeSnapshot {
    ffi::NativeSnapshot {
        version: value.version,
        id: value.id,
        total_buckets: value.total_buckets,
        families: value.column_family_ids.into_iter().map(family).collect(),
        shards: value
            .shard_snapshots
            .into_iter()
            .map(|shard| ffi::NativeShardSnapshot {
                ranges: shard
                    .ranges
                    .into_iter()
                    .map(|range| ffi::NativeRange {
                        first: *range.start(),
                        last: *range.end(),
                    })
                    .collect(),
                families: shard.column_family_ids.into_iter().map(family).collect(),
                db_id: shard.db_id,
                snapshot_id: shard.snapshot_id,
                manifest_path: shard.manifest_path,
                timestamp_seconds: shard.timestamp_seconds,
                data_size_bytes: shard.data_size_bytes,
                incremental_data_size_bytes: shard.incremental_data_size_bytes,
            })
            .collect(),
        watermark_seconds: value.watermark_seconds,
    }
}

pub(crate) fn native_database_start_snapshot(
    db: &NativeDatabase,
) -> BridgeResult<Box<NativePendingSnapshot>> {
    let (sender, receiver) = mpsc::channel();
    let id = db
        .db
        .snapshot_with_callback(move |result| {
            let _ = sender.send(result.map_err(format_cobble_error));
        })
        .map_err(format_cobble_error)?;
    Ok(Box::new(NativePendingSnapshot {
        id,
        receiver: Some(receiver),
    }))
}

pub(crate) fn native_pending_snapshot_id(pending: &NativePendingSnapshot) -> u64 {
    pending.id
}

pub(crate) fn native_pending_snapshot_wait(
    pending: &mut NativePendingSnapshot,
) -> BridgeResult<ffi::NativeSnapshot> {
    let receiver = pending
        .receiver
        .take()
        .ok_or_else(|| input_error("pending snapshot was already waited"))?;
    receiver
        .recv()
        .map_err(|_| input_error("snapshot completion channel closed"))?
        .map(snapshot)
}

pub(crate) fn native_database_take_snapshot(
    db: &NativeDatabase,
) -> BridgeResult<ffi::NativeSnapshot> {
    let mut pending = native_database_start_snapshot(db)?;
    native_pending_snapshot_wait(&mut pending)
}

pub(crate) fn native_database_get_snapshot_typed(
    db: &NativeDatabase,
    id: u64,
) -> BridgeResult<ffi::NativeSnapshot> {
    db.db
        .get_snapshot(id)
        .map(snapshot)
        .map_err(format_cobble_error)
}

pub(crate) fn native_database_list_snapshots_typed(
    db: &NativeDatabase,
) -> BridgeResult<Vec<ffi::NativeSnapshot>> {
    db.db
        .list_snapshots()
        .map(|snapshots| snapshots.into_iter().map(snapshot).collect())
        .map_err(format_cobble_error)
}
