use std::ops::RangeInclusive;
use std::sync::Arc;
use std::sync::mpsc;
use std::time::Duration;

use crate::structured_bridge::ffi;

use super::conversion::{format_error, input_error};
use super::{
    BridgeResult, NativePendingShardSnapshot, NativePendingSnapshot, NativeStructuredDb,
    NativeStructuredSingleDb,
};

pub(crate) fn native_structured_db_set_time(db: &NativeStructuredDb, unix_seconds: u32) {
    db.db.set_time(unix_seconds);
}
pub(crate) fn native_structured_db_now_seconds(db: &NativeStructuredDb) -> u32 {
    db.db.now_seconds()
}
pub(crate) fn native_structured_db_snapshot(db: &NativeStructuredDb) -> BridgeResult<u64> {
    db.db.snapshot().map_err(format_error)
}
pub(crate) fn native_structured_db_close(db: &NativeStructuredDb) -> BridgeResult<()> {
    db.db.close().map_err(format_error)
}
pub(crate) fn native_structured_single_db_set_time(
    db: &NativeStructuredSingleDb,
    unix_seconds: u32,
) {
    db.db.set_time(unix_seconds);
}
pub(crate) fn native_structured_single_db_now_seconds(db: &NativeStructuredSingleDb) -> u32 {
    db.db.db().db().now_seconds()
}
pub(crate) fn native_structured_single_db_snapshot(
    db: &NativeStructuredSingleDb,
) -> BridgeResult<u64> {
    db.db.snapshot().map_err(format_error)
}
pub(crate) fn native_structured_single_db_close(db: &NativeStructuredSingleDb) -> BridgeResult<()> {
    db.db.close().map_err(format_error)
}

fn native_family((name, id): (String, u8)) -> ffi::NativeFamily {
    ffi::NativeFamily { name, id }
}

fn native_shard_snapshot(value: cobble_binding::ShardSnapshotInput) -> ffi::NativeShardSnapshot {
    ffi::NativeShardSnapshot {
        ranges: value
            .ranges
            .into_iter()
            .map(|range| ffi::NativeBucketRange {
                start_inclusive: *range.start(),
                end_inclusive: *range.end(),
            })
            .collect(),
        families: value
            .column_family_ids
            .into_iter()
            .map(native_family)
            .collect(),
        db_id: value.db_id,
        snapshot_id: value.snapshot_id,
        manifest_path: value.manifest_path,
        timestamp_seconds: value.timestamp_seconds,
        data_size_bytes: value.data_size_bytes,
        incremental_data_size_bytes: value.incremental_data_size_bytes,
    }
}

fn native_shard_snapshot_ref(value: cobble_binding::ShardSnapshotRef) -> ffi::NativeShardSnapshot {
    ffi::NativeShardSnapshot {
        ranges: value
            .ranges
            .into_iter()
            .map(|range| ffi::NativeBucketRange {
                start_inclusive: *range.start(),
                end_inclusive: *range.end(),
            })
            .collect(),
        families: value
            .column_family_ids
            .into_iter()
            .map(native_family)
            .collect(),
        db_id: value.db_id,
        snapshot_id: value.snapshot_id,
        manifest_path: value.manifest_path,
        timestamp_seconds: value.timestamp_seconds,
        data_size_bytes: value.data_size_bytes,
        incremental_data_size_bytes: value.incremental_data_size_bytes,
    }
}

fn native_snapshot(value: cobble_binding::GlobalSnapshotManifest) -> ffi::NativeSnapshot {
    ffi::NativeSnapshot {
        version: value.version,
        id: value.id,
        total_buckets: value.total_buckets,
        families: value
            .column_family_ids
            .into_iter()
            .map(native_family)
            .collect(),
        shards: value
            .shard_snapshots
            .into_iter()
            .map(native_shard_snapshot_ref)
            .collect(),
        watermark_seconds: value.watermark_seconds,
    }
}

pub(crate) fn native_structured_db_start_snapshot(
    db: &NativeStructuredDb,
) -> BridgeResult<Box<NativePendingShardSnapshot>> {
    let (sender, receiver) = mpsc::channel();
    let id = db
        .db
        .snapshot_with_callback(move |result| {
            let _ = sender.send(result.map_err(format_error));
        })
        .map_err(format_error)?;
    Ok(Box::new(NativePendingShardSnapshot {
        id,
        receiver: Some(receiver),
    }))
}

pub(crate) fn native_pending_shard_snapshot_id(pending: &NativePendingShardSnapshot) -> u64 {
    pending.id
}

pub(crate) fn native_pending_shard_snapshot_wait(
    pending: &mut NativePendingShardSnapshot,
) -> BridgeResult<ffi::NativeShardSnapshot> {
    pending
        .receiver
        .take()
        .ok_or_else(|| input_error("pending shard snapshot was already waited"))?
        .recv()
        .map_err(|_| input_error("shard snapshot completion channel closed"))?
        .map(native_shard_snapshot)
}

pub(crate) fn native_structured_db_take_snapshot(
    db: &NativeStructuredDb,
) -> BridgeResult<ffi::NativeShardSnapshot> {
    let mut pending = native_structured_db_start_snapshot(db)?;
    native_pending_shard_snapshot_wait(&mut pending)
}

pub(crate) fn native_structured_db_cancel_snapshot(
    db: &NativeStructuredDb,
    snapshot_id: u64,
) -> BridgeResult<bool> {
    db.db.cancel_snapshot(snapshot_id).map_err(format_error)
}

pub(crate) fn native_structured_db_get_shard_snapshot(
    db: &NativeStructuredDb,
    snapshot_id: u64,
) -> BridgeResult<ffi::NativeShardSnapshot> {
    db.db
        .shard_snapshot_input(snapshot_id)
        .map(native_shard_snapshot)
        .map_err(format_error)
}

pub(crate) fn native_structured_db_retain_snapshot(
    db: &NativeStructuredDb,
    snapshot_id: u64,
) -> bool {
    db.db.retain_snapshot(snapshot_id)
}

pub(crate) fn native_structured_db_expire_snapshot(
    db: &NativeStructuredDb,
    snapshot_id: u64,
) -> BridgeResult<bool> {
    db.db.expire_snapshot(snapshot_id).map_err(format_error)
}

pub(crate) fn native_structured_db_switch_to_snapshot(
    db: &mut NativeStructuredDb,
    snapshot_id: u64,
) -> BridgeResult<()> {
    Arc::get_mut(&mut db.db)
        .ok_or_else(|| {
            "CB_INVALID_STATE: SwitchToSnapshot requires exclusive ownership; release every structured scan cursor, schema builder, and priority queue first".to_owned()
        })?
        .switch_to_snapshot(snapshot_id)
        .map_err(format_error)
}

pub(crate) fn native_structured_single_db_start_snapshot(
    db: &NativeStructuredSingleDb,
) -> BridgeResult<Box<NativePendingSnapshot>> {
    let (sender, receiver) = mpsc::channel();
    let id = db
        .db
        .snapshot_with_callback(move |result| {
            let _ = sender.send(result.map_err(format_error));
        })
        .map_err(format_error)?;
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
    pending
        .receiver
        .take()
        .ok_or_else(|| input_error("pending snapshot was already waited"))?
        .recv()
        .map_err(|_| input_error("snapshot completion channel closed"))?
        .map(native_snapshot)
}

pub(crate) fn native_structured_single_db_take_snapshot(
    db: &NativeStructuredSingleDb,
) -> BridgeResult<ffi::NativeSnapshot> {
    let mut pending = native_structured_single_db_start_snapshot(db)?;
    native_pending_snapshot_wait(&mut pending)
}

pub(crate) fn native_structured_single_db_list_snapshots(
    db: &NativeStructuredSingleDb,
) -> BridgeResult<Vec<ffi::NativeSnapshot>> {
    db.db
        .list_snapshots()
        .map(|values| values.into_iter().map(native_snapshot).collect())
        .map_err(format_error)
}

pub(crate) fn native_structured_single_db_retain_snapshot(
    db: &NativeStructuredSingleDb,
    snapshot_id: u64,
) -> BridgeResult<bool> {
    db.db.retain_snapshot(snapshot_id).map_err(format_error)
}

pub(crate) fn native_structured_single_db_expire_snapshot(
    db: &NativeStructuredSingleDb,
    snapshot_id: u64,
) -> BridgeResult<bool> {
    db.db.expire_snapshot(snapshot_id).map_err(format_error)
}

fn memtable_type(value: u8) -> BridgeResult<cobble_binding::MemtableType> {
    match value {
        0 => Ok(cobble_binding::MemtableType::Hash),
        1 => Ok(cobble_binding::MemtableType::Skiplist),
        2 => Ok(cobble_binding::MemtableType::Vec),
        3 => Ok(cobble_binding::MemtableType::Adaptive),
        _ => Err(input_error("unknown memtable type")),
    }
}

pub(crate) fn native_structured_db_switch_memtable_type(
    db: &NativeStructuredDb,
    value: u8,
    flush_current: bool,
) -> BridgeResult<()> {
    db.db
        .switch_memtable_type(memtable_type(value)?, flush_current)
        .map_err(format_error)
}
pub(crate) fn native_structured_single_db_switch_memtable_type(
    db: &NativeStructuredSingleDb,
    value: u8,
    flush_current: bool,
) -> BridgeResult<()> {
    db.db
        .switch_memtable_type(memtable_type(value)?, flush_current)
        .map_err(format_error)
}
pub(crate) fn native_structured_db_load_readonly_files_to_primary(
    db: &NativeStructuredDb,
) -> BridgeResult<u64> {
    db.db
        .load_readonly_files_to_primary()
        .map_err(format_error)
        .and_then(|value| {
            u64::try_from(value).map_err(|_| input_error("readonly file count exceeds u64"))
        })
}
pub(crate) fn native_structured_single_db_load_readonly_files_to_primary(
    db: &NativeStructuredSingleDb,
) -> BridgeResult<u64> {
    db.db
        .load_readonly_files_to_primary()
        .map_err(format_error)
        .and_then(|value| {
            u64::try_from(value).map_err(|_| input_error("readonly file count exceeds u64"))
        })
}

pub(crate) fn native_structured_db_metrics(db: &NativeStructuredDb) -> Vec<ffi::NativeMetric> {
    db.db
        .metrics()
        .into_iter()
        .map(|sample| {
            let labels = sample
                .labels
                .into_iter()
                .map(|(key, value)| ffi::NativeMetricLabel { key, value })
                .collect();
            match sample.value {
                cobble_binding::MetricValue::Counter(value) => ffi::NativeMetric {
                    name: sample.name,
                    labels,
                    kind: 0,
                    counter: value,
                    gauge: 0.0,
                    count: 0,
                    sum: 0.0,
                    min: 0.0,
                    max: 0.0,
                },
                cobble_binding::MetricValue::Gauge(value) => ffi::NativeMetric {
                    name: sample.name,
                    labels,
                    kind: 1,
                    counter: 0,
                    gauge: value,
                    count: 0,
                    sum: 0.0,
                    min: 0.0,
                    max: 0.0,
                },
                cobble_binding::MetricValue::Histogram(value) => ffi::NativeMetric {
                    name: sample.name,
                    labels,
                    kind: 2,
                    counter: 0,
                    gauge: 0.0,
                    count: value.count,
                    sum: value.sum,
                    min: value.min,
                    max: value.max,
                },
            }
        })
        .collect()
}

fn expand_storage_mode(value: u8) -> BridgeResult<cobble_binding::ExpandStorageMode> {
    match value {
        0 => Ok(cobble_binding::ExpandStorageMode::AdoptAsync),
        1 => Ok(cobble_binding::ExpandStorageMode::ReferencePersistent),
        2 => Ok(cobble_binding::ExpandStorageMode::ReferencePersistentWithCache),
        _ => Err(input_error("unknown expand storage mode")),
    }
}

fn unchecked_ranges(
    values: Vec<ffi::NativeBucketRange>,
    operation: &str,
) -> BridgeResult<Vec<RangeInclusive<u16>>> {
    if values.is_empty() {
        return Err(input_error(format!("{operation} ranges must not be empty")));
    }
    values
        .into_iter()
        .map(|value| {
            if value.start_inclusive > value.end_inclusive {
                return Err(input_error(format!("{operation} range is reversed")));
            }
            Ok(value.start_inclusive..=value.end_inclusive)
        })
        .collect()
}

pub(crate) fn native_structured_db_expand_bucket(
    db: &NativeStructuredDb,
    source_db_id: &str,
    has_snapshot_id: bool,
    snapshot_id: u64,
    has_ranges: bool,
    values: Vec<ffi::NativeBucketRange>,
    storage_mode: u8,
) -> BridgeResult<u64> {
    let ranges = has_ranges
        .then(|| unchecked_ranges(values, "expand"))
        .transpose()?;
    db.db
        .expand_bucket_with_storage_mode(
            source_db_id,
            has_snapshot_id.then_some(snapshot_id),
            ranges,
            expand_storage_mode(storage_mode)?,
        )
        .map_err(format_error)
}
pub(crate) fn native_structured_db_wait_for_expand_adoption(
    db: &NativeStructuredDb,
    timeout_millis: i64,
) -> BridgeResult<()> {
    let millis =
        u64::try_from(timeout_millis).map_err(|_| input_error("timeout must not be negative"))?;
    db.db
        .wait_for_expand_adoption(Duration::from_millis(millis))
        .map_err(format_error)
}
pub(crate) fn native_structured_db_shrink_bucket(
    db: &NativeStructuredDb,
    values: Vec<ffi::NativeBucketRange>,
) -> BridgeResult<u64> {
    db.db
        .shrink_bucket(unchecked_ranges(values, "shrink")?)
        .map_err(format_error)
}
