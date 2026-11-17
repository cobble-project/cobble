//! Per-operation read, scan, and write options with schema-aware caches.

use crate::ColumnFamilyOptions;
use crate::error::{Error, Result};
use crate::schema::Schema;
use crate::util::{column_mask_last_byte, size_to_usize};
use arc_swap::ArcSwapOption;
use size::Size;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct ReadOptions {
    pub column_family: Option<String>,
    pub column_indices: Option<Vec<usize>>,
    max_index: Option<usize>,
    cached_masks: Arc<ArcSwapOption<ReadOptionsMasks>>,
    cached_column_family_id: Arc<ArcSwapOption<ColumnFamilyCacheEntry>>,
    schema_binding: Option<Arc<ColumnFamilySchemaBinding>>,
}

#[derive(Clone)]
pub struct ScanOptions {
    pub read_ahead_bytes: Size,
    pub column_indices: Option<Vec<usize>>,
    pub column_family: Option<String>,
    preload_scan_cursor_block: bool,
    should_stop_at_block_boundary: bool,
    max_index: Option<usize>,
    max_rows: Option<usize>,
    cached_resolution: Arc<ArcSwapOption<ScanOptionsCacheEntry>>,
    schema_binding: Option<Arc<ColumnFamilySchemaBinding>>,
}

#[derive(Clone, Debug)]
pub struct WriteOptions {
    pub ttl_seconds: Option<u32>,
    pub column_family: Option<String>,
    /// When WAL is enabled, wait until this write's WAL segment is durably published.
    ///
    /// Disabling this keeps the write immediately visible in the current process, but a crash
    /// may lose the not-yet-published WAL tail.
    pub await_durable: bool,
    cached_column_family_id: Arc<ArcSwapOption<ColumnFamilyCacheEntry>>,
    schema_binding: Option<Arc<ColumnFamilySchemaBinding>>,
}

#[derive(Clone, Debug)]
struct ColumnFamilyCacheEntry {
    schema_version: u64,
    column_family_id: u8,
}

/// A higher-level binding to the physical definition of one column family.
///
/// Options carrying this binding validate it whenever their column-family resolution cache sees
/// a new schema version. This lets structured wrappers reject stale layouts at the same point the
/// core selects the schema for an operation, without affecting unbound KV options.
#[derive(Debug)]
struct ColumnFamilySchemaBinding {
    options: ColumnFamilyOptions,
    num_columns: usize,
}

impl ColumnFamilySchemaBinding {
    fn validate(&self, schema: &Schema, column_family_id: u8) -> Result<()> {
        let actual_columns = schema.num_columns_in_family(column_family_id).unwrap_or(0);
        let actual_options = schema.column_family_options_ref(column_family_id);
        if actual_columns != self.num_columns || actual_options != Some(&self.options) {
            return Err(Error::InvalidState(
                "Bound column family layout or metadata is stale; refresh the table and rebuild projections"
                    .to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone)]
pub(crate) struct ScanOptionsResolved {
    pub(crate) column_family_id: u8,
    pub(crate) effective_schema: Arc<Schema>,
}

#[derive(Clone)]
struct ScanOptionsCacheEntry {
    schema_version: u64,
    resolved: ScanOptionsResolved,
}

impl WriteOptions {
    pub fn with_ttl(ttl_seconds: u32) -> Self {
        Self {
            ttl_seconds: Some(ttl_seconds),
            column_family: None,
            await_durable: true,
            cached_column_family_id: Arc::new(ArcSwapOption::empty()),
            schema_binding: None,
        }
    }

    pub fn with_column_family(column_family: impl Into<String>) -> Self {
        Self {
            ttl_seconds: None,
            column_family: Some(column_family.into()),
            await_durable: true,
            cached_column_family_id: Arc::new(ArcSwapOption::empty()),
            schema_binding: None,
        }
    }

    /// Sets whether a WAL-backed write waits for durable publication before returning.
    pub fn with_await_durable(mut self, await_durable: bool) -> Self {
        self.await_durable = await_durable;
        self
    }

    /// Return equivalent options bound to a column family with a fresh resolution cache.
    pub fn bound_to_column_family(&self, column_family: impl Into<String>) -> Self {
        Self {
            ttl_seconds: self.ttl_seconds,
            column_family: Some(column_family.into()),
            await_durable: self.await_durable,
            cached_column_family_id: Arc::new(ArcSwapOption::empty()),
            schema_binding: self.schema_binding.clone(),
        }
    }

    /// Bind this option set to one higher-level column-family layout.
    #[doc(hidden)]
    pub fn bound_to_column_family_schema(
        mut self,
        options: ColumnFamilyOptions,
        num_columns: usize,
    ) -> Self {
        self.schema_binding = Some(Arc::new(ColumnFamilySchemaBinding {
            options,
            num_columns,
        }));
        self.cached_column_family_id = Arc::new(ArcSwapOption::empty());
        self
    }

    pub(crate) fn column_family(&self) -> Option<&str> {
        self.column_family.as_deref()
    }

    pub(crate) fn resolve_column_family_id_cached(&self, schema: &Schema) -> Result<u8> {
        let schema_version = schema.version();
        if let Some(cache) = self.cached_column_family_id.load_full()
            && cache.schema_version == schema_version
        {
            return Ok(cache.column_family_id);
        }
        let column_family_id = schema.resolve_column_family_id(self.column_family())?;
        if let Some(binding) = &self.schema_binding {
            binding.validate(schema, column_family_id)?;
        }
        self.cached_column_family_id
            .store(Some(Arc::new(ColumnFamilyCacheEntry {
                schema_version,
                column_family_id,
            })));
        Ok(column_family_id)
    }

    pub(crate) fn validate_bound_column_family_cached(
        &self,
        schema: &Schema,
        column_family_id: u8,
    ) -> Result<()> {
        if self.schema_binding.is_none() {
            return Ok(());
        }
        let resolved = self.resolve_column_family_id_cached(schema)?;
        if resolved != column_family_id {
            return Err(Error::InvalidState(format!(
                "Bound column family resolves to {}, not {}",
                resolved, column_family_id
            )));
        }
        Ok(())
    }

    pub(crate) fn has_schema_binding(&self) -> bool {
        self.schema_binding.is_some()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ReadOptionsMasks {
    pub(crate) num_columns: usize,
    pub(crate) selected_mask: Option<Arc<[u8]>>,
    pub(crate) base_mask: Arc<[u8]>,
}

impl Default for ReadOptions {
    fn default() -> Self {
        Self {
            column_family: None,
            column_indices: None,
            max_index: None,
            cached_masks: Arc::new(ArcSwapOption::empty()),
            cached_column_family_id: Arc::new(ArcSwapOption::empty()),
            schema_binding: None,
        }
    }
}

impl Default for ScanOptions {
    fn default() -> Self {
        Self {
            read_ahead_bytes: Size::from_const(0),
            column_indices: None,
            column_family: None,
            preload_scan_cursor_block: false,
            should_stop_at_block_boundary: false,
            max_index: None,
            max_rows: None,
            cached_resolution: Arc::new(ArcSwapOption::empty()),
            schema_binding: None,
        }
    }
}

impl std::fmt::Debug for ScanOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScanOptions")
            .field("read_ahead_bytes", &self.read_ahead_bytes)
            .field("column_indices", &self.column_indices)
            .field("column_family", &self.column_family)
            .field("preload_scan_cursor_block", &self.preload_scan_cursor_block)
            .field(
                "should_stop_at_block_boundary",
                &self.should_stop_at_block_boundary,
            )
            .field("max_index", &self.max_index)
            .field("max_rows", &self.max_rows)
            .finish()
    }
}

impl Default for WriteOptions {
    fn default() -> Self {
        Self {
            ttl_seconds: None,
            column_family: None,
            await_durable: true,
            cached_column_family_id: Arc::new(ArcSwapOption::empty()),
            schema_binding: None,
        }
    }
}

impl ScanOptions {
    pub fn for_column(column_index: usize) -> Self {
        Self::new_with_indices(Some(vec![column_index]))
    }

    pub fn for_columns(column_indices: Vec<usize>) -> Self {
        Self::new_with_indices(Some(column_indices))
    }

    fn new_with_indices(column_indices: Option<Vec<usize>>) -> Self {
        let max_index = column_indices
            .as_ref()
            .and_then(|indices| indices.iter().max().cloned());
        Self {
            read_ahead_bytes: Size::from_const(0),
            column_indices,
            column_family: None,
            preload_scan_cursor_block: false,
            should_stop_at_block_boundary: false,
            max_index,
            max_rows: None,
            cached_resolution: Arc::new(ArcSwapOption::empty()),
            schema_binding: None,
        }
    }

    pub fn with_column_family(mut self, column_family: impl Into<String>) -> Self {
        self.column_family = Some(column_family.into());
        self.invalidate_caches();
        self
    }

    /// Bind this option set to one higher-level column-family layout.
    #[doc(hidden)]
    pub fn bound_to_column_family_schema(
        mut self,
        options: ColumnFamilyOptions,
        num_columns: usize,
    ) -> Self {
        self.schema_binding = Some(Arc::new(ColumnFamilySchemaBinding {
            options,
            num_columns,
        }));
        self.invalidate_caches();
        self
    }

    pub fn with_max_rows(mut self, max_rows: usize) -> Self {
        assert!(max_rows > 0, "max_rows must be > 0");
        self.max_rows = Some(max_rows);
        self
    }

    /// Preload the next SST data block while a scan advances.
    ///
    /// This is intentionally opt-in: short cursor-driven scans such as priority-queue
    /// polling benefit from keeping the block after the cursor warm, while broad scans
    /// should avoid the extra synchronous cache lookup by default.
    pub fn with_preload_scan_cursor_block(mut self, enabled: bool) -> Self {
        self.preload_scan_cursor_block = enabled;
        self
    }

    /// Stop scanning after crossing the next physical block boundary.
    ///
    /// This is primarily useful for batch-oriented consumers such as priority
    /// queues that want each poll to stay close to the storage layout instead of
    /// reading arbitrarily far ahead. A stop is surfaced through the iterator's
    /// `stopped_at_block_boundary()` signal, and callers must clear that signal
    /// before resuming iteration.
    ///
    /// The exact boundary depends on the underlying reader: SST scans stop at
    /// data-block boundaries, Parquet scans stop at row-group boundaries, and
    /// wrapper iterators propagate that event upward.
    pub fn with_stop_at_block_boundary(mut self, enabled: bool) -> Self {
        self.should_stop_at_block_boundary = enabled;
        self
    }

    pub(crate) fn column_family(&self) -> Option<&str> {
        self.column_family.as_deref()
    }

    pub(crate) fn columns(&self) -> Option<&[usize]> {
        self.column_indices.as_deref()
    }

    pub(crate) fn max_index(&self) -> Option<usize> {
        self.max_index
    }

    pub fn max_rows(&self) -> Option<usize> {
        self.max_rows
    }

    pub fn preload_scan_cursor_block(&self) -> bool {
        self.preload_scan_cursor_block
    }

    /// Whether boundary-aware stopping is enabled for scans created from these
    /// options.
    ///
    /// When this returns `true`, the scan iterator may pause after a physical
    /// storage boundary and report that pause through
    /// `KvIterator::stopped_at_block_boundary()`.
    pub fn should_stop_at_block_boundary(&self) -> bool {
        self.should_stop_at_block_boundary
    }

    pub fn set_max_rows(&mut self, max_rows: usize) {
        assert!(max_rows > 0, "max_rows must be > 0");
        self.max_rows = Some(max_rows);
    }

    pub fn clear_max_rows(&mut self) {
        self.max_rows = None;
    }

    pub fn set_preload_scan_cursor_block(&mut self, enabled: bool) {
        self.preload_scan_cursor_block = enabled;
    }

    pub(crate) fn read_ahead_bytes(&self) -> Result<usize> {
        size_to_usize("scan.read_ahead_bytes", self.read_ahead_bytes).map_err(Error::ConfigError)
    }

    pub(crate) fn resolve_cached(&self, schema: &Arc<Schema>) -> Result<ScanOptionsResolved> {
        let schema_version = schema.version();
        if let Some(cache) = self.cached_resolution.load_full()
            && cache.schema_version == schema_version
        {
            return Ok(cache.resolved.clone());
        }

        let column_family_id = schema.resolve_column_family_id(self.column_family())?;
        if let Some(binding) = &self.schema_binding {
            binding.validate(schema.as_ref(), column_family_id)?;
        }
        let effective_schema = if let Some(columns) = self.columns() {
            schema.project_in_family(column_family_id, columns)
        } else {
            Arc::clone(schema)
        };
        let resolved = ScanOptionsResolved {
            column_family_id,
            effective_schema,
        };
        self.cached_resolution
            .store(Some(Arc::new(ScanOptionsCacheEntry {
                schema_version,
                resolved: resolved.clone(),
            })));
        Ok(resolved)
    }

    fn invalidate_caches(&mut self) {
        self.cached_resolution = Arc::new(ArcSwapOption::empty());
    }
}

impl ReadOptions {
    pub fn for_column(column_index: usize) -> Self {
        Self::new_with_indices(None, Some(vec![column_index]))
    }

    pub fn for_columns(column_indices: Vec<usize>) -> Self {
        Self::new_with_indices(None, Some(column_indices))
    }

    pub fn for_column_in_family(column_family: impl Into<String>, column_index: usize) -> Self {
        Self::new_with_indices(Some(column_family.into()), Some(vec![column_index]))
    }

    pub fn for_columns_in_family(
        column_family: impl Into<String>,
        column_indices: Vec<usize>,
    ) -> Self {
        Self::new_with_indices(Some(column_family.into()), Some(column_indices))
    }

    fn new_with_indices(column_family: Option<String>, column_indices: Option<Vec<usize>>) -> Self {
        let max_index = column_indices
            .as_ref()
            .and_then(|indices| indices.iter().max().cloned());
        Self {
            column_family,
            column_indices,
            max_index,
            cached_masks: Arc::new(ArcSwapOption::empty()),
            cached_column_family_id: Arc::new(ArcSwapOption::empty()),
            schema_binding: None,
        }
    }

    pub fn with_column_family(mut self, column_family: impl Into<String>) -> Self {
        self.column_family = Some(column_family.into());
        self.invalidate_caches();
        self
    }

    /// Bind this option set to one higher-level column-family layout.
    #[doc(hidden)]
    pub fn bound_to_column_family_schema(
        mut self,
        options: ColumnFamilyOptions,
        num_columns: usize,
    ) -> Self {
        self.schema_binding = Some(Arc::new(ColumnFamilySchemaBinding {
            options,
            num_columns,
        }));
        self.invalidate_caches();
        self
    }

    pub(crate) fn columns(&self) -> Option<&[usize]> {
        self.column_indices.as_deref()
    }

    pub(crate) fn column_family(&self) -> Option<&str> {
        self.column_family.as_deref()
    }

    pub(crate) fn max_index(&self) -> Option<usize> {
        self.max_index
    }

    pub(crate) fn masks(&self, num_columns: usize) -> ReadOptionsMasks {
        if let Some(mask) = self.cached_masks.load_full()
            && mask.num_columns == num_columns
        {
            return mask.as_ref().clone();
        }
        let mask = Arc::new(self.build_masks(num_columns));
        self.cached_masks.store(Some(Arc::clone(&mask)));
        mask.as_ref().clone()
    }

    pub(crate) fn resolve_column_family_id_cached(&self, schema: &Schema) -> Result<u8> {
        let schema_version = schema.version();
        if let Some(cache) = self.cached_column_family_id.load_full()
            && cache.schema_version == schema_version
        {
            return Ok(cache.column_family_id);
        }
        let column_family_id = schema.resolve_column_family_id(self.column_family())?;
        if let Some(binding) = &self.schema_binding {
            binding.validate(schema, column_family_id)?;
        }
        self.cached_column_family_id
            .store(Some(Arc::new(ColumnFamilyCacheEntry {
                schema_version,
                column_family_id,
            })));
        Ok(column_family_id)
    }

    fn invalidate_caches(&mut self) {
        self.cached_masks = Arc::new(ArcSwapOption::empty());
        self.cached_column_family_id = Arc::new(ArcSwapOption::empty());
    }

    fn build_masks(&self, num_columns: usize) -> ReadOptionsMasks {
        let mask_size = num_columns.div_ceil(8).max(1);
        let last_mask = column_mask_last_byte(num_columns);
        let selected_mask = self.column_indices.as_ref().map(|columns| {
            let mut mask = vec![0u8; mask_size];
            for &column_idx in columns {
                if column_idx < num_columns {
                    mask[column_idx / 8] |= 1 << (column_idx % 8);
                }
            }
            mask[mask_size - 1] &= last_mask;
            Arc::from(mask.into_boxed_slice())
        });
        let base_mask = if let Some(mask) = selected_mask.as_ref() {
            Arc::clone(mask)
        } else {
            let mut mask = vec![0xFF; mask_size];
            mask[mask_size - 1] &= last_mask;
            Arc::from(mask.into_boxed_slice())
        };
        ReadOptionsMasks {
            num_columns,
            selected_mask,
            base_mask,
        }
    }
}
