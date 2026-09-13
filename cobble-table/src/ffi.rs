//! Feature-gated bridge for language bindings.
//!
//! Connector-specific encoded operations live here so the semantic table API
//! does not expose raw storage rows.

use crate::{Result, TableScanSplit};
use cobble::{Config, ScanSplitScanner};

/// Open a table-aware projected scan while retaining encoded key/value rows.
///
/// The split's fixed metadata and built-in schema transforms are applied before
/// the raw scanner is returned to the language binding.
pub fn open_projected_scan(
    split: &TableScanSplit,
    runtime: Config,
    field_names: &[String],
    read_ahead_bytes: i64,
) -> Result<ScanSplitScanner> {
    split.create_projected_raw_scanner(runtime, field_names, read_ahead_bytes)
}
