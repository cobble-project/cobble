//! Feature-gated bridge for language bindings.
//!
//! Connector-specific encoded operations live here so the semantic table API
//! does not expose raw storage rows.

use crate::table::TypedRead;
use crate::{ReadOnlyTable, Result, Table, TableReader, TableScanSplit, TableSchema};
use bytes::Bytes;
use cobble::{ColumnFamilyOptions, Config, DbIterator, ReadOptions, ScanOptions, ScanSplitScanner};
use std::sync::Arc;

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

/// A fixed raw view acquired from a global typed table reader.
///
/// This is connector-only: it keeps the matching compiled layout and global
/// snapshot alive while a binding encodes keys and performs raw I/O.
#[derive(Clone)]
pub struct TableReaderView {
    pub(crate) typed: Arc<TypedRead>,
}

impl TableReaderView {
    pub fn schema(&self) -> &TableSchema {
        self.typed.schema()
    }

    pub fn schema_binding(&self) -> TableSchemaBinding {
        self.typed.ffi_schema_binding()
    }

    pub fn total_buckets(&self) -> u32 {
        self.typed
            .global_state()
            .expect("global table reader view")
            .total_buckets()
    }

    pub fn get(
        &self,
        bucket: u16,
        key: &[u8],
        options: &ReadOptions,
    ) -> Result<Option<Vec<Option<Bytes>>>> {
        self.state().get(bucket, key, options)
    }

    pub fn multi_get(
        &self,
        keys: &[(u16, &[u8])],
        options: &ReadOptions,
    ) -> Result<Vec<Option<Vec<Option<Bytes>>>>> {
        self.state().multi_get(keys, options)
    }

    pub fn scan(
        &self,
        bucket: u16,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        options: &ScanOptions,
    ) -> Result<DbIterator> {
        self.state().scan(bucket, start, end, options)
    }

    fn state(&self) -> &crate::runtime::GlobalReaderState {
        self.typed
            .global_state()
            .expect("global table reader view")
            .as_ref()
    }
}

/// Capture the reader's current stable raw view without I/O.
pub fn acquire_table_reader_view(reader: &TableReader) -> TableReaderView {
    reader.ffi_acquire_view()
}

/// Acquire a new raw view only when it differs from the binding's current one.
pub fn acquire_table_reader_view_if_changed(
    reader: &TableReader,
    current: &TableReaderView,
) -> Option<TableReaderView> {
    reader.ffi_acquire_view_if_changed(current)
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
