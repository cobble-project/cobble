use cobble_binding::{Config, ScanSplit};

use crate::{
    BridgeResult,
    error::{format_cobble_error, input_error},
    ffi,
    options::to_scan_options,
    scan::{NativeScanCursor, native_scan_cursor_from_split_scanner},
    snapshot::{shard_snapshot_ref, shard_snapshot_reference},
};

fn native_split(value: ScanSplit) -> BridgeResult<ffi::NativeScanSplit> {
    let (has_start_after, start_after_bucket, start_after_key) =
        match (value.start_bucket, value.start_key_exclusive) {
            (Some(bucket), Some(key)) => (true, bucket, key),
            (None, None) => (false, 0, Vec::new()),
            _ => {
                return Err(input_error(
                    "scan split start bucket and key must be set together",
                ));
            }
        };
    let (has_end_at, end_at_bucket, end_at_key) = match (value.end_bucket, value.end_key_inclusive)
    {
        (Some(bucket), Some(key)) => (true, bucket, key),
        (None, None) => (false, 0, Vec::new()),
        _ => {
            return Err(input_error(
                "scan split end bucket and key must be set together",
            ));
        }
    };
    Ok(ffi::NativeScanSplit {
        shard: shard_snapshot_ref(value.shard),
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

fn split(value: ffi::NativeScanSplit) -> BridgeResult<ScanSplit> {
    Ok(ScanSplit {
        shard: shard_snapshot_reference(value.shard)?,
        start: value.has_start.then_some(value.start),
        end: value.has_end.then_some(value.end),
        start_bucket: value.has_start_after.then_some(value.start_after_bucket),
        start_key_exclusive: value.has_start_after.then_some(value.start_after_key),
        end_bucket: value.has_end_at.then_some(value.end_at_bucket),
        end_key_inclusive: value.has_end_at.then_some(value.end_at_key),
    })
}

pub(crate) fn native_scan_split_split_after(
    value: ffi::NativeScanSplit,
    bucket: u16,
    key: &[u8],
) -> BridgeResult<Vec<ffi::NativeScanSplit>> {
    let partition = split(value)?
        .split_after(bucket, key.to_vec())
        .map_err(format_cobble_error)?;
    Ok(vec![
        native_split(partition.before)?,
        native_split(partition.after)?,
    ])
}

pub(crate) fn native_scan_split_to_json(value: ffi::NativeScanSplit) -> BridgeResult<String> {
    serde_json::to_string(&split(value)?)
        .map_err(|error| input_error(&format!("cannot encode scan split JSON: {error}")))
}

pub(crate) fn native_scan_split_from_json(json: &str) -> BridgeResult<ffi::NativeScanSplit> {
    let value: ScanSplit = serde_json::from_str(json)
        .map_err(|error| input_error(&format!("invalid scan split JSON: {error}")))?;
    native_split(value)
}

fn open_scanner(
    config: Config,
    value: ffi::NativeScanSplit,
    options: &ffi::NativeScanOptions,
) -> BridgeResult<Box<NativeScanCursor>> {
    if options.stop_at_block_boundary {
        return Err(input_error(
            "stop_at_block_boundary is not supported for split scanners",
        ));
    }
    opendal::install_default();
    let scanner = split(value)?
        .create_scanner(config, &to_scan_options(options)?)
        .map_err(format_cobble_error)?;
    Ok(native_scan_cursor_from_split_scanner(scanner))
}

pub(crate) fn native_scan_split_open_scanner(
    config_json: &str,
    value: ffi::NativeScanSplit,
    options: &ffi::NativeScanOptions,
) -> BridgeResult<Box<NativeScanCursor>> {
    open_scanner(
        Config::from_json_str(config_json).map_err(format_cobble_error)?,
        value,
        options,
    )
}

pub(crate) fn native_scan_split_open_scanner_file(
    config_path: &str,
    value: ffi::NativeScanSplit,
    options: &ffi::NativeScanOptions,
) -> BridgeResult<Box<NativeScanCursor>> {
    open_scanner(
        Config::from_path(config_path).map_err(format_cobble_error)?,
        value,
        options,
    )
}
