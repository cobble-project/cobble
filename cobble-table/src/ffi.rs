//! Feature-gated bridge for language bindings.
//!
//! Connector-specific encoded operations live here so the semantic table API
//! does not expose raw storage rows.

use crate::{ReadOnlyTable, Result, Table, TableScanSplit};
use cobble::{ColumnFamilyOptions, Config, ScanSplitScanner};

/// Exact physical column-family definition captured with a typed table view.
///
/// Language bindings use this only to bind their private core read/write/scan
/// options. It prevents a stale typed layout from being resolved against a
/// later core schema without exposing a generic options API.
pub struct TableSchemaBinding {
    pub(crate) options: ColumnFamilyOptions,
    pub(crate) physical_columns: usize,
}

impl TableSchemaBinding {
    pub fn column_family_options(&self) -> &ColumnFamilyOptions {
        &self.options
    }

    pub fn physical_columns(&self) -> usize {
        self.physical_columns
    }
}

/// Return the physical definition captured by this writable typed table.
pub fn table_schema_binding(table: &Table) -> TableSchemaBinding {
    table.ffi_schema_binding()
}

/// Return the physical definition captured by this fixed snapshot table.
pub fn read_only_table_schema_binding(table: &ReadOnlyTable) -> TableSchemaBinding {
    table.ffi_schema_binding()
}

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
