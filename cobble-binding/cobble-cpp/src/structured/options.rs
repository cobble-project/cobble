use cobble_binding::structured::{StructuredReadOptions, StructuredScanOptions};

use super::conversion::input_error;
use super::{BridgeResult, NativeStructuredReadOptions, NativeStructuredScanOptions};

pub(crate) fn native_structured_read_options_new() -> Box<NativeStructuredReadOptions> {
    Box::new(NativeStructuredReadOptions {
        options: StructuredReadOptions::default(),
    })
}

pub(crate) fn native_structured_read_options_clone(
    options: &NativeStructuredReadOptions,
) -> Box<NativeStructuredReadOptions> {
    Box::new(NativeStructuredReadOptions {
        options: options.options.clone(),
    })
}

pub(crate) fn native_structured_read_options_set_family(
    options: &mut NativeStructuredReadOptions,
    has_family: bool,
    family: &str,
) -> BridgeResult<()> {
    if has_family && family.is_empty() {
        return Err(input_error("column family must not be empty"));
    }
    let mut raw = options.options.clone().into_cobble();
    raw.column_family = has_family.then(|| family.to_string());
    options.options = raw.into();
    Ok(())
}

pub(crate) fn native_structured_read_options_set_columns(
    options: &mut NativeStructuredReadOptions,
    columns: Vec<u64>,
) -> BridgeResult<()> {
    let columns = columns
        .into_iter()
        .map(|column| usize::try_from(column).map_err(|_| input_error("column exceeds usize")))
        .collect::<BridgeResult<Vec<_>>>()?;
    let mut raw = options.options.clone().into_cobble();
    raw.column_indices = (!columns.is_empty()).then_some(columns);
    options.options = StructuredReadOptions::from(raw);
    Ok(())
}

pub(crate) fn native_structured_scan_options_new() -> Box<NativeStructuredScanOptions> {
    Box::new(NativeStructuredScanOptions {
        options: StructuredScanOptions::default(),
    })
}

pub(crate) fn native_structured_scan_options_clone(
    options: &NativeStructuredScanOptions,
) -> Box<NativeStructuredScanOptions> {
    Box::new(NativeStructuredScanOptions {
        options: options.options.clone(),
    })
}

pub(crate) fn native_structured_scan_options_set_family(
    options: &mut NativeStructuredScanOptions,
    has_family: bool,
    family: &str,
) -> BridgeResult<()> {
    if has_family && family.is_empty() {
        return Err(input_error("column family must not be empty"));
    }
    let mut raw = options.options.clone().into_cobble();
    raw.column_family = has_family.then(|| family.to_string());
    options.options = StructuredScanOptions::from(raw);
    Ok(())
}

pub(crate) fn native_structured_scan_options_set_columns(
    options: &mut NativeStructuredScanOptions,
    columns: Vec<u64>,
) -> BridgeResult<()> {
    let columns = columns
        .into_iter()
        .map(|column| usize::try_from(column).map_err(|_| input_error("column exceeds usize")))
        .collect::<BridgeResult<Vec<_>>>()?;
    let mut raw = options.options.clone().into_cobble();
    raw.column_indices = (!columns.is_empty()).then_some(columns);
    options.options = StructuredScanOptions::from(raw);
    Ok(())
}

pub(crate) fn native_structured_scan_options_set_preload(
    options: &mut NativeStructuredScanOptions,
    enabled: bool,
) {
    options.options = options
        .options
        .clone()
        .with_preload_scan_cursor_block(enabled);
}

pub(crate) fn native_structured_scan_options_set_stop_at_block_boundary(
    options: &mut NativeStructuredScanOptions,
    enabled: bool,
) {
    options.options = options.options.clone().with_stop_at_block_boundary(enabled);
}
