use std::collections::BTreeMap;
use std::ops::RangeInclusive;

use cobble_binding::Config;
use cobble_binding::structured::StructuredScanSplit;

use crate::structured_bridge::ffi;

use super::conversion::{format_error, input_error};
use super::scan::{NativeStructuredScanCursor, native_structured_scan_cursor_from_split};
use super::{BridgeResult, NativeStructuredScanOptions};

fn native_split(value: StructuredScanSplit) -> BridgeResult<ffi::NativeStructuredScanSplit> {
    let (has_start_after, start_after_bucket, start_after_key) =
        match (value.start_bucket, value.start_key_exclusive) {
            (Some(bucket), Some(key)) => (true, bucket, key),
            (None, None) => (false, 0, Vec::new()),
            _ => {
                return Err(input_error(
                    "structured split start bucket and key must be set together",
                ));
            }
        };
    let (has_end_at, end_at_bucket, end_at_key) = match (value.end_bucket, value.end_key_inclusive)
    {
        (Some(bucket), Some(key)) => (true, bucket, key),
        (None, None) => (false, 0, Vec::new()),
        _ => {
            return Err(input_error(
                "structured split end bucket and key must be set together",
            ));
        }
    };
    Ok(ffi::NativeStructuredScanSplit {
        shard: native_shard(value.shard),
        has_start: value.start.is_some(),
        start: value.start.unwrap_or_default(),
        has_end: value.end.is_some(),
        end: value.end.unwrap_or_default(),
        has_start_after,
        start_after_bucket,
        start_after_key,
        has_end_at,
        end_at_bucket,
        end_at_key,
    })
}

fn split(value: ffi::NativeStructuredScanSplit) -> BridgeResult<StructuredScanSplit> {
    Ok(StructuredScanSplit {
        shard: shard(value.shard)?,
        start: value.has_start.then_some(value.start),
        end: value.has_end.then_some(value.end),
        start_bucket: value.has_start_after.then_some(value.start_after_bucket),
        start_key_exclusive: value.has_start_after.then_some(value.start_after_key),
        end_bucket: value.has_end_at.then_some(value.end_at_bucket),
        end_key_inclusive: value.has_end_at.then_some(value.end_at_key),
    })
}

pub(crate) fn native_structured_scan_split_split_after(
    value: ffi::NativeStructuredScanSplit,
    bucket: u16,
    key: &[u8],
) -> BridgeResult<Vec<ffi::NativeStructuredScanSplit>> {
    let partition = split(value)?
        .split_after(bucket, key.to_vec())
        .map_err(format_error)?;
    Ok(vec![
        native_split(partition.before)?,
        native_split(partition.after)?,
    ])
}

pub(crate) fn native_structured_scan_split_to_json(
    value: ffi::NativeStructuredScanSplit,
) -> BridgeResult<String> {
    serde_json::to_string(&split(value)?)
        .map_err(|error| input_error(format!("cannot encode structured split JSON: {error}")))
}

pub(crate) fn native_structured_scan_split_from_json(
    json: &str,
) -> BridgeResult<ffi::NativeStructuredScanSplit> {
    let value: StructuredScanSplit = serde_json::from_str(json)
        .map_err(|error| input_error(format!("invalid structured split JSON: {error}")))?;
    native_split(value)
}

fn open_scanner(
    config: Config,
    value: ffi::NativeStructuredScanSplit,
    options: &NativeStructuredScanOptions,
) -> BridgeResult<Box<NativeStructuredScanCursor>> {
    if options.options.as_cobble().should_stop_at_block_boundary() {
        return Err(input_error(
            "stop_at_block_boundary is not supported for structured split scanners",
        ));
    }
    opendal::install_default();
    let scanner = split(value)?
        .create_scanner(config, &options.options)
        .map_err(format_error)?;
    Ok(native_structured_scan_cursor_from_split(scanner))
}

pub(crate) fn native_structured_scan_split_open_scanner(
    config_json: &str,
    value: ffi::NativeStructuredScanSplit,
    options: &NativeStructuredScanOptions,
) -> BridgeResult<Box<NativeStructuredScanCursor>> {
    open_scanner(
        Config::from_json_str(config_json).map_err(format_error)?,
        value,
        options,
    )
}

pub(crate) fn native_structured_scan_split_open_scanner_file(
    config_path: &str,
    value: ffi::NativeStructuredScanSplit,
    options: &NativeStructuredScanOptions,
) -> BridgeResult<Box<NativeStructuredScanCursor>> {
    open_scanner(
        Config::from_path(config_path).map_err(format_error)?,
        value,
        options,
    )
}

fn native_shard(value: cobble_binding::ShardSnapshotRef) -> ffi::NativeShardSnapshot {
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
            .map(|(name, id)| ffi::NativeFamily { name, id })
            .collect(),
        db_id: value.db_id,
        snapshot_id: value.snapshot_id,
        manifest_path: value.manifest_path,
        timestamp_seconds: value.timestamp_seconds,
        data_size_bytes: value.data_size_bytes,
        incremental_data_size_bytes: value.incremental_data_size_bytes,
    }
}

fn shard(value: ffi::NativeShardSnapshot) -> BridgeResult<cobble_binding::ShardSnapshotRef> {
    if value.db_id.is_empty() || value.manifest_path.is_empty() {
        return Err(input_error(
            "structured split shard db_id and manifest_path must not be empty",
        ));
    }
    Ok(cobble_binding::ShardSnapshotRef {
        ranges: ranges(value.ranges)?,
        column_family_ids: families(value.families)?,
        db_id: value.db_id,
        snapshot_id: value.snapshot_id,
        manifest_path: value.manifest_path,
        timestamp_seconds: value.timestamp_seconds,
        data_size_bytes: value.data_size_bytes,
        incremental_data_size_bytes: value.incremental_data_size_bytes,
    })
}

fn ranges(values: Vec<ffi::NativeBucketRange>) -> BridgeResult<Vec<RangeInclusive<u16>>> {
    if values.is_empty() {
        return Err(input_error(
            "structured split shard ranges must not be empty",
        ));
    }
    values
        .into_iter()
        .map(|range| {
            if range.start_inclusive > range.end_inclusive {
                return Err(input_error("structured split shard range is reversed"));
            }
            Ok(range.start_inclusive..=range.end_inclusive)
        })
        .collect()
}

fn families(values: Vec<ffi::NativeFamily>) -> BridgeResult<BTreeMap<String, u8>> {
    let mut by_name = BTreeMap::new();
    let mut by_id = BTreeMap::new();
    for family in values {
        if family.name.is_empty() {
            return Err(input_error("column family name must not be empty"));
        }
        if by_name.insert(family.name.clone(), family.id).is_some() {
            return Err(input_error("duplicate column family name"));
        }
        if by_id.insert(family.id, family.name).is_some() {
            return Err(input_error("duplicate column family id"));
        }
    }
    Ok(by_name)
}
