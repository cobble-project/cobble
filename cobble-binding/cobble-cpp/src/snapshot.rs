use std::{collections::BTreeMap, ops::RangeInclusive, sync::mpsc};

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

pub(crate) struct NativePendingShardSnapshot {
    id: u64,
    receiver: Option<mpsc::Receiver<BridgeResult<cobble_binding::ShardSnapshotMetadata>>>,
}

fn family((name, id): (String, u8)) -> ffi::NativeFamily {
    ffi::NativeFamily { name, id }
}

fn schema_families(
    families: std::collections::BTreeMap<String, cobble_binding::SnapshotColumnFamily>,
) -> Vec<ffi::NativeSnapshotColumnFamily> {
    families
        .into_iter()
        .map(|(name, family)| ffi::NativeSnapshotColumnFamily {
            name,
            id: family.id,
            num_columns: family.num_columns,
            value_has_ttl: family.options.value_has_ttl,
            metadata_json: family
                .options
                .metadata
                .map(|value| value.to_string())
                .unwrap_or_default(),
        })
        .collect()
}

pub(crate) fn shard_snapshot(
    value: cobble_binding::ShardSnapshotMetadata,
) -> ffi::NativeShardSnapshot {
    let families = value.column_family_ids().into_iter().map(family).collect();
    ffi::NativeShardSnapshot {
        ranges: value
            .ranges
            .into_iter()
            .map(|range| ffi::NativeRange {
                first: *range.start(),
                last: *range.end(),
            })
            .collect(),
        families,
        db_id: value.db_id,
        snapshot_id: value.snapshot_id,
        manifest_path: value.manifest_path,
        timestamp_seconds: value.timestamp_seconds,
        data_size_bytes: value.data_size_bytes,
        incremental_data_size_bytes: value.incremental_data_size_bytes,
        has_schema_metadata: true,
        schema_id: value.schema_id,
        schema_families: schema_families(value.column_families),
    }
}

pub(crate) fn shard_snapshot_ref(
    value: cobble_binding::ShardSnapshotRef,
) -> ffi::NativeShardSnapshot {
    ffi::NativeShardSnapshot {
        ranges: value
            .ranges
            .into_iter()
            .map(|range| ffi::NativeRange {
                first: *range.start(),
                last: *range.end(),
            })
            .collect(),
        families: value.column_family_ids.into_iter().map(family).collect(),
        db_id: value.db_id,
        snapshot_id: value.snapshot_id,
        manifest_path: value.manifest_path,
        timestamp_seconds: value.timestamp_seconds,
        data_size_bytes: value.data_size_bytes,
        incremental_data_size_bytes: value.incremental_data_size_bytes,
        has_schema_metadata: false,
        schema_id: 0,
        schema_families: Vec::new(),
    }
}

pub(crate) fn snapshot(value: cobble_binding::GlobalSnapshotManifest) -> ffi::NativeSnapshot {
    ffi::NativeSnapshot {
        version: value.version,
        id: value.id,
        total_buckets: value.total_buckets,
        families: value.column_family_ids.into_iter().map(family).collect(),
        shards: value
            .shard_snapshots
            .into_iter()
            .map(shard_snapshot_ref)
            .collect(),
        watermark_seconds: value.watermark_seconds,
    }
}

fn native_families(values: Vec<ffi::NativeFamily>) -> BridgeResult<BTreeMap<String, u8>> {
    let mut by_name = BTreeMap::new();
    let mut by_id = BTreeMap::new();
    for value in values {
        if value.name.is_empty() {
            return Err(input_error("column family name must not be empty"));
        }
        if by_name.insert(value.name.clone(), value.id).is_some() {
            return Err(input_error("duplicate column family name"));
        }
        if by_id.insert(value.id, value.name.clone()).is_some() {
            return Err(input_error("duplicate column family id"));
        }
    }
    Ok(by_name)
}

fn native_ranges(values: Vec<ffi::NativeRange>) -> BridgeResult<Vec<RangeInclusive<u16>>> {
    if values.is_empty() {
        return Err(input_error("shard snapshot ranges must not be empty"));
    }
    values
        .into_iter()
        .map(|value| {
            if value.first > value.last {
                return Err(input_error("shard snapshot range is reversed"));
            }
            Ok(value.first..=value.last)
        })
        .collect()
}

pub(crate) fn shard_snapshot_metadata(
    value: ffi::NativeShardSnapshot,
) -> BridgeResult<cobble_binding::ShardSnapshotMetadata> {
    if value.db_id.is_empty() || value.manifest_path.is_empty() {
        return Err(input_error(
            "shard snapshot db_id and manifest_path must not be empty",
        ));
    }
    if !value.has_schema_metadata {
        return Err(input_error("shard snapshot schema metadata is required"));
    }
    let mut column_families = BTreeMap::new();
    for family in value.schema_families {
        let metadata = (!family.metadata_json.is_empty())
            .then(|| serde_json::from_str(&family.metadata_json))
            .transpose()
            .map_err(|error| {
                input_error(&format!("invalid column family metadata json: {error}"))
            })?;
        column_families.insert(
            family.name,
            cobble_binding::SnapshotColumnFamily {
                id: family.id,
                num_columns: family.num_columns,
                options: cobble_binding::ColumnFamilyOptions {
                    value_has_ttl: family.value_has_ttl,
                    metadata,
                },
            },
        );
    }
    Ok(cobble_binding::ShardSnapshotMetadata {
        ranges: native_ranges(value.ranges)?,
        db_id: value.db_id,
        snapshot_id: value.snapshot_id,
        manifest_path: value.manifest_path,
        timestamp_seconds: value.timestamp_seconds,
        data_size_bytes: value.data_size_bytes,
        incremental_data_size_bytes: value.incremental_data_size_bytes,
        schema_id: value.schema_id,
        column_families,
    })
}

pub(crate) fn shard_snapshot_reference(
    value: ffi::NativeShardSnapshot,
) -> BridgeResult<cobble_binding::ShardSnapshotRef> {
    Ok(cobble_binding::ShardSnapshotRef {
        ranges: native_ranges(value.ranges)?,
        column_family_ids: native_families(value.families)?,
        db_id: value.db_id,
        snapshot_id: value.snapshot_id,
        manifest_path: value.manifest_path,
        timestamp_seconds: value.timestamp_seconds,
        data_size_bytes: value.data_size_bytes,
        incremental_data_size_bytes: value.incremental_data_size_bytes,
    })
}

pub(crate) fn native_sharded_database_snapshot(
    db: &crate::sharded_db::NativeShardedDatabase,
) -> BridgeResult<u64> {
    db.db.snapshot().map_err(format_cobble_error)
}

pub(crate) fn native_sharded_database_start_snapshot(
    db: &crate::sharded_db::NativeShardedDatabase,
) -> BridgeResult<Box<NativePendingShardSnapshot>> {
    let (sender, receiver) = mpsc::channel();
    let id = db
        .db
        .snapshot_with_callback(move |result| {
            let _ = sender.send(result.map_err(format_cobble_error));
        })
        .map_err(format_cobble_error)?;
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
    let receiver = pending
        .receiver
        .take()
        .ok_or_else(|| input_error("pending shard snapshot was already waited"))?;
    receiver
        .recv()
        .map_err(|_| input_error("shard snapshot completion channel closed"))?
        .map(shard_snapshot)
}

pub(crate) fn native_sharded_database_take_snapshot(
    db: &crate::sharded_db::NativeShardedDatabase,
) -> BridgeResult<ffi::NativeShardSnapshot> {
    let mut pending = native_sharded_database_start_snapshot(db)?;
    native_pending_shard_snapshot_wait(&mut pending)
}

pub(crate) fn native_sharded_database_cancel_snapshot(
    db: &crate::sharded_db::NativeShardedDatabase,
    snapshot_id: u64,
) -> BridgeResult<bool> {
    db.db
        .cancel_snapshot(snapshot_id)
        .map_err(format_cobble_error)
}

pub(crate) fn native_sharded_database_get_shard_snapshot(
    db: &crate::sharded_db::NativeShardedDatabase,
    snapshot_id: u64,
) -> BridgeResult<ffi::NativeShardSnapshot> {
    db.db
        .shard_snapshot_metadata(snapshot_id)
        .map(shard_snapshot)
        .map_err(format_cobble_error)
}

pub(crate) fn native_sharded_database_retain_snapshot(
    db: &crate::sharded_db::NativeShardedDatabase,
    snapshot_id: u64,
) -> bool {
    db.db.retain_snapshot(snapshot_id)
}

pub(crate) fn native_sharded_database_expire_snapshot(
    db: &crate::sharded_db::NativeShardedDatabase,
    snapshot_id: u64,
) -> BridgeResult<bool> {
    db.db
        .expire_snapshot(snapshot_id)
        .map_err(format_cobble_error)
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
