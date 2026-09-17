//! Feature-gated bridge for language bindings.
//!
//! Connector-specific encoded operations live here so the semantic table API
//! does not expose raw storage rows.

use crate::table::{ReadBackend, TypedRead};
use crate::{ReadOnlyTable, Result, Table, TableReader, TableScanSplit, TableSchema};
use bytes::Bytes;
use cobble::{
    ColumnFamilyOptions, Config, DbIterator, ReadOptions, ScanOptions, ScanSplitScanner,
    WriteOptions,
};
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

/// Connector-only raw access to one captured table layout.
///
/// This owns the bound core options and the read backend. Cloning it creates another fixed read
/// view without retaining the writable table handle itself.
#[derive(Clone)]
pub struct RawTableAccess {
    backend: ReadBackend,
    read_options: ReadOptions,
    scan_options: ScanOptions,
}

/// Connector-owned writable table handle.
///
/// The typed table owns the database Arc and its bound options. A raw access clone freezes a
/// matching read layout for projections and cursors without retaining Java's caller Db facade.
pub struct TableHandle {
    table: Table,
    access: RawTableAccess,
}

impl TableHandle {
    pub fn new(table: Table) -> Self {
        let access = table.ffi_raw_access();
        Self { table, access }
    }

    pub fn schema(&self) -> &TableSchema {
        self.table.schema()
    }

    pub fn name(&self) -> &str {
        self.table.name()
    }

    pub fn total_buckets(&self) -> u32 {
        self.table.db().total_buckets()
    }

    pub fn metrics(&self) -> Vec<cobble::MetricSample> {
        self.table.metrics()
    }

    pub fn direct_buffer_pool_config(&self) -> Result<(usize, usize)> {
        cobble::ffi::db_direct_buffer_pool_config(self.table.db()).map_err(Into::into)
    }

    pub fn schema_binding(&self) -> TableSchemaBinding {
        self.table.ffi_schema_binding()
    }

    pub fn access(&self) -> &RawTableAccess {
        &self.access
    }

    pub fn projection(&self, field_names: &[String]) -> Result<RawTableAccess> {
        self.table.ffi_raw_projection(field_names)
    }

    pub fn put_columns(&self, bucket: u16, key: &[u8], columns: &[&[u8]]) -> Result<()> {
        self.table.db().put_columns_with_options(
            bucket,
            key,
            columns,
            self.table.ffi_write_options(),
        )?;
        Ok(())
    }

    pub fn put_columns_with_options(
        &self,
        bucket: u16,
        key: &[u8],
        columns: &[&[u8]],
        options: &WriteOptions,
    ) -> Result<()> {
        let bound = self.table.rebound_write_options(options);
        self.table
            .db()
            .put_columns_with_options(bucket, key, columns, &bound)?;
        Ok(())
    }

    pub fn delete(&self, bucket: u16, key: &[u8]) -> Result<()> {
        self.table
            .db()
            .delete_row_with_options(bucket, key, self.table.ffi_write_options())?;
        Ok(())
    }

    pub fn delete_batch(&self, keys: &[(u16, &[u8])]) -> Result<()> {
        self.table
            .db()
            .delete_rows_with_options(keys, self.table.ffi_write_options())?;
        Ok(())
    }

    pub fn snapshot_with_callback<F>(&self, callback: F) -> Result<u64>
    where
        F: Fn(cobble::Result<cobble::ShardSnapshotMetadata>) + Send + Sync + 'static,
    {
        self.table.snapshot_with_callback(callback)
    }

    pub fn shard_snapshot_metadata(
        &self,
        snapshot_id: u64,
    ) -> Result<cobble::ShardSnapshotMetadata> {
        self.table.shard_snapshot_metadata(snapshot_id)
    }

    pub fn refresh(&mut self) -> Result<bool> {
        let changed = self.table.refresh_schema()?;
        if changed {
            self.access = self.table.ffi_raw_access();
        }
        Ok(changed)
    }

    #[doc(hidden)]
    pub fn refresh_from_catalog(
        &mut self,
        catalog: &crate::catalog::CatalogTable,
    ) -> crate::catalog::CatalogResult<bool> {
        let changed = catalog.refresh_writer(&mut self.table)?;
        self.access = self.table.ffi_raw_access();
        Ok(changed)
    }
}

impl RawTableAccess {
    pub(crate) fn new(
        backend: ReadBackend,
        read_options: ReadOptions,
        scan_options: ScanOptions,
    ) -> Self {
        Self {
            backend,
            read_options,
            scan_options,
        }
    }

    pub fn get(&self, bucket: u16, key: &[u8]) -> Result<Option<Vec<Option<Bytes>>>> {
        self.backend
            .get_with_options(bucket, key, &self.read_options)
    }

    pub fn multi_get(&self, keys: &[(u16, &[u8])]) -> Result<Vec<Option<Vec<Option<Bytes>>>>> {
        self.backend
            .multi_get_with_options(keys, &self.read_options)
    }

    pub fn scan(
        &self,
        bucket: u16,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> Result<DbIterator> {
        self.backend
            .scan_with_options_bounds(bucket, start, end, &self.scan_options)
    }
}

/// Return the physical definition captured by this fixed snapshot table.
pub fn read_only_table_schema_binding(table: &ReadOnlyTable) -> TableSchemaBinding {
    table.ffi_schema_binding()
}

/// Clone the snapshot database owned by a fixed read-only table.
///
/// This is connector-only so bindings can transfer ownership of a table-builder result without
/// exposing a general database accessor on the typed table API.
pub fn read_only_table_db(table: &ReadOnlyTable) -> Arc<cobble::ReadOnlyDb> {
    table.ffi_shard_db()
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

    /// Build a portable scan plan from this fixed global reader view.
    pub fn scan_plan(&self) -> Result<crate::TableScanPlan> {
        self.state().scan_plan()
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
