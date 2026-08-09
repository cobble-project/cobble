use crate::priority_queue::{
    PriorityQueue, priority_queue_column_family_name, priority_queue_column_family_options,
    validate_priority_queue_column_family,
};
use crate::structured_db::{
    StructuredColumnValue, StructuredDbIterator, StructuredReadOptions, StructuredScanOptions,
    StructuredSchema, StructuredSchemaBuilder, StructuredSchemaOwner, StructuredWriteBatch,
    StructuredWriteOptions, decode_row, encode_for_write,
    load_structured_schema_from_cobble_schema, persist_structured_schema_on_db,
};
use cobble::{Config, DbIterator, Error, MemtableType, Result, SingleDb};
use std::ops::Range;
use std::sync::Arc;

pub struct StructuredSingleDb {
    db: SingleDb,
    structured_schema: Arc<StructuredSchema>,
    default_write_options: StructuredWriteOptions,
    default_read_options: StructuredReadOptions,
    default_scan_options: StructuredScanOptions,
}

impl StructuredSingleDb {
    pub fn open(config: Config) -> Result<Self> {
        let db = SingleDb::open(config)?;
        let structured_schema =
            load_structured_schema_from_cobble_schema(&db.db().current_schema())?;
        Ok(Self {
            db,
            structured_schema: Arc::new(structured_schema),
            default_write_options: StructuredWriteOptions::default(),
            default_read_options: StructuredReadOptions::default(),
            default_scan_options: StructuredScanOptions::default(),
        })
    }

    pub fn db(&self) -> &SingleDb {
        &self.db
    }

    pub fn jni_direct_buffer_pool_config(&self) -> Result<(usize, usize)> {
        self.db.db().jni_direct_buffer_pool_config()
    }

    pub fn current_schema(&self) -> StructuredSchema {
        self.structured_schema.as_ref().clone()
    }

    pub fn update_schema(&mut self) -> StructuredSchemaBuilder<'_, Self> {
        StructuredSchemaBuilder::new(self)
    }

    /// Creates a structured priority queue backed by a dedicated column family.
    ///
    /// The returned queue uses the same `PriorityQueue` API as sharded `StructuredDb`, and the
    /// queue itself owns the backend-specific dispatch. This keeps upper layers from having to
    /// special-case `StructuredSingleDb` when offering, scanning, or advancing queue items.
    pub fn new_priority_queue<'a>(
        &'a mut self,
        name: impl Into<String>,
    ) -> Result<PriorityQueue<'a>> {
        let normalized_name = priority_queue_column_family_name(name.into())?;
        if self
            .db
            .db()
            .current_schema()
            .column_family_ids()
            .contains_key(normalized_name.as_str())
        {
            return Err(Error::InvalidState(format!(
                "priority queue '{}' already exists",
                normalized_name
            )));
        }

        let mut builder = self.update_schema();
        builder.add_bytes_column(Some(normalized_name.clone()), 0);
        builder.set_column_family_options(
            Some(normalized_name.clone()),
            priority_queue_column_family_options(),
        );
        builder.commit()?;
        let column_family_id = self
            .current_schema()
            .resolve_column_family_id(Some(normalized_name.as_str()))?;

        Ok(PriorityQueue::from_single_column_family(
            self,
            normalized_name,
            column_family_id,
        ))
    }

    /// Opens an existing structured priority queue by name.
    ///
    /// The target column family must exist, be marked as a priority queue, and contain exactly one
    /// bytes column.
    pub fn get_priority_queue<'a>(&'a self, name: impl Into<String>) -> Result<PriorityQueue<'a>> {
        let normalized_name = priority_queue_column_family_name(name.into())?;
        let column_family_id = validate_priority_queue_column_family(
            self.db.db().current_schema().as_ref(),
            normalized_name.as_str(),
        )?;
        Ok(PriorityQueue::from_single_column_family(
            self,
            normalized_name,
            column_family_id,
        ))
    }

    /// Opens an existing structured priority queue or creates it on first use.
    ///
    /// If a same-named column family already exists, it must already be a valid priority queue
    /// family.
    pub fn get_or_new_priority_queue<'a>(
        &'a mut self,
        name: impl Into<String>,
    ) -> Result<PriorityQueue<'a>> {
        let normalized_name = priority_queue_column_family_name(name.into())?;
        if self
            .db
            .db()
            .current_schema()
            .column_family_ids()
            .contains_key(normalized_name.as_str())
        {
            let column_family_id = validate_priority_queue_column_family(
                self.db.db().current_schema().as_ref(),
                normalized_name.as_str(),
            )?;
            return Ok(PriorityQueue::from_single_column_family(
                self,
                normalized_name,
                column_family_id,
            ));
        }

        let mut builder = self.update_schema();
        builder.add_bytes_column(Some(normalized_name.clone()), 0);
        builder.set_column_family_options(
            Some(normalized_name.clone()),
            priority_queue_column_family_options(),
        );
        builder.commit()?;
        let column_family_id = self
            .current_schema()
            .resolve_column_family_id(Some(normalized_name.as_str()))?;

        Ok(PriorityQueue::from_single_column_family(
            self,
            normalized_name,
            column_family_id,
        ))
    }
}

impl StructuredSingleDb {
    fn reset_default_options(&mut self) {
        self.default_write_options = StructuredWriteOptions::default();
        self.default_read_options = StructuredReadOptions::default();
        self.default_scan_options = StructuredScanOptions::default();
    }

    pub fn reload_schema(&mut self) -> Result<()> {
        let schema = load_structured_schema_from_cobble_schema(&self.db.db().current_schema())?;
        self.structured_schema = Arc::new(schema);
        self.reset_default_options();
        Ok(())
    }

    pub fn apply_schema(
        &mut self,
        structured_schema: StructuredSchema,
    ) -> Result<StructuredSchema> {
        persist_structured_schema_on_db(self.db.db(), &structured_schema)?;
        let reloaded = load_structured_schema_from_cobble_schema(&self.db.db().current_schema())?;
        self.structured_schema = Arc::new(reloaded.clone());
        self.reset_default_options();
        Ok(reloaded)
    }

    // ── Write operations ────────────────────────────────────────────────

    pub fn put<K, V>(&self, bucket: u16, key: K, column: u16, value: V) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: Into<StructuredColumnValue>,
    {
        self.put_with_options(bucket, key, column, value, &self.default_write_options)
    }

    pub fn put_with_options<K, V>(
        &self,
        bucket: u16,
        key: K,
        column: u16,
        value: V,
        options: &StructuredWriteOptions,
    ) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: Into<StructuredColumnValue>,
    {
        let encoded = encode_for_write(
            &self.structured_schema,
            options.column_family(),
            self.db.db().now_seconds(),
            column,
            value.into(),
            options.ttl_seconds(),
        )?;
        self.db
            .put_with_options(bucket, key, column, encoded, options.as_cobble())
    }

    pub fn merge<K, V>(&self, bucket: u16, key: K, column: u16, value: V) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: Into<StructuredColumnValue>,
    {
        self.merge_with_options(bucket, key, column, value, &self.default_write_options)
    }

    pub fn merge_with_options<K, V>(
        &self,
        bucket: u16,
        key: K,
        column: u16,
        value: V,
        options: &StructuredWriteOptions,
    ) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: Into<StructuredColumnValue>,
    {
        let encoded = encode_for_write(
            &self.structured_schema,
            options.column_family(),
            self.db.db().now_seconds(),
            column,
            value.into(),
            options.ttl_seconds(),
        )?;
        self.db
            .merge_with_options(bucket, key, column, encoded, options.as_cobble())
    }

    pub fn delete<K>(&self, bucket: u16, key: K, column: u16) -> Result<()>
    where
        K: AsRef<[u8]>,
    {
        self.delete_with_options(bucket, key, column, &self.default_write_options)
    }

    pub fn delete_with_options<K>(
        &self,
        bucket: u16,
        key: K,
        column: u16,
        options: &StructuredWriteOptions,
    ) -> Result<()>
    where
        K: AsRef<[u8]>,
    {
        self.db
            .delete_with_options(bucket, key, column, options.as_cobble())
    }

    pub fn new_write_batch(&self) -> StructuredWriteBatch {
        StructuredWriteBatch::new(
            Arc::clone(&self.structured_schema),
            self.db.db().now_seconds(),
        )
    }

    pub fn write_batch(&self, batch: StructuredWriteBatch) -> Result<()> {
        self.db.write_batch(batch.into_inner())
    }

    pub fn write_batch_with_options(
        &self,
        batch: StructuredWriteBatch,
        options: &StructuredWriteOptions,
    ) -> Result<()> {
        self.db
            .write_batch_with_options(batch.into_inner(), options.as_cobble())
    }

    // ── Read operations ─────────────────────────────────────────────────

    pub fn get<K>(&self, bucket: u16, key: K) -> Result<Option<Vec<Option<StructuredColumnValue>>>>
    where
        K: AsRef<[u8]>,
    {
        self.get_with_options(bucket, key, &self.default_read_options)
    }

    pub fn multi_get<K>(
        &self,
        keys: &[(u16, K)],
    ) -> Result<Vec<Option<Vec<Option<StructuredColumnValue>>>>>
    where
        K: AsRef<[u8]>,
    {
        self.multi_get_with_options(keys, &self.default_read_options)
    }

    pub fn multi_get_with_options<K>(
        &self,
        keys: &[(u16, K)],
        options: &StructuredReadOptions,
    ) -> Result<Vec<Option<Vec<Option<StructuredColumnValue>>>>>
    where
        K: AsRef<[u8]>,
    {
        let raw_keys = keys
            .iter()
            .map(|(bucket, key)| (*bucket, key.as_ref()))
            .collect::<Vec<_>>();
        let projected_schema = options.resolve_projected_schema_cached(&self.structured_schema)?;
        self.db
            .multi_get_with_options(&raw_keys, options.as_cobble())?
            .into_iter()
            .map(|raw| {
                raw.map(|columns| decode_row(&projected_schema, 0, columns))
                    .transpose()
            })
            .collect()
    }

    pub fn get_with_options<K>(
        &self,
        bucket: u16,
        key: K,
        options: &StructuredReadOptions,
    ) -> Result<Option<Vec<Option<StructuredColumnValue>>>>
    where
        K: AsRef<[u8]>,
    {
        let raw = self
            .db
            .get_with_options(bucket, key.as_ref(), options.as_cobble())?;
        let projected_schema = options.resolve_projected_schema_cached(&self.structured_schema)?;
        raw.map(|columns| decode_row(&projected_schema, 0, columns))
            .transpose()
    }

    pub fn scan<'a>(
        &'a self,
        bucket: u16,
        range: Range<&[u8]>,
    ) -> Result<StructuredDbIterator<'a>> {
        self.scan_with_options(bucket, range, &self.default_scan_options)
    }

    pub fn scan_with_options<'a>(
        &'a self,
        bucket: u16,
        range: Range<&[u8]>,
        options: &StructuredScanOptions,
    ) -> Result<StructuredDbIterator<'a>> {
        let inner = self
            .db
            .scan_with_options(bucket, range, options.as_cobble())?;
        let projected_schema = options.resolve_projected_schema_cached(&self.structured_schema)?;
        Ok(StructuredDbIterator::new(inner, projected_schema, 0))
    }

    pub fn scan_raw_bounds<'a>(
        &'a self,
        bucket: u16,
        start_key_inclusive: Option<&[u8]>,
        end_key_exclusive: Option<&[u8]>,
        options: &StructuredScanOptions,
    ) -> Result<DbIterator<'a>> {
        self.db.db().scan_with_options_bounds(
            bucket,
            start_key_inclusive,
            end_key_exclusive,
            options.as_cobble(),
        )
    }

    // ── Snapshot lifecycle ───────────────────────────────────────────────

    pub fn snapshot(&self) -> Result<u64> {
        self.db.snapshot()
    }

    /// Change the memtable implementation for future rotations.
    ///
    /// `flush_current = false` leaves the active memtable untouched; `true` rotates a non-empty
    /// active memtable now through the normal flush path.
    pub fn switch_memtable_type(
        &self,
        memtable_type: MemtableType,
        flush_current: bool,
    ) -> Result<()> {
        self.db.switch_memtable_type(memtable_type, flush_current)
    }

    /// Mark all currently referenced READONLY files for asynchronous loading into primary
    /// storage.
    pub fn load_readonly_files_to_primary(&self) -> Result<usize> {
        self.db.load_readonly_files_to_primary()
    }

    pub fn snapshot_with_callback<F>(&self, callback: F) -> Result<u64>
    where
        F: Fn(Result<cobble::GlobalSnapshotManifest>) + Send + Sync + 'static,
    {
        self.db.snapshot_with_callback(callback)
    }

    pub fn retain_snapshot(&self, global_snapshot_id: u64) -> Result<bool> {
        self.db.retain_snapshot(global_snapshot_id)
    }

    pub fn expire_snapshot(&self, global_snapshot_id: u64) -> Result<bool> {
        self.db.expire_snapshot(global_snapshot_id)
    }

    pub fn list_snapshots(&self) -> Result<Vec<cobble::GlobalSnapshotManifest>> {
        self.db.list_snapshots()
    }

    pub fn set_time(&self, next: u32) {
        self.db.set_time(next)
    }

    pub fn close(&self) -> Result<()> {
        self.db.close()
    }

    pub(crate) fn advance_column_family_truncation_cursor_by_id(
        &self,
        bucket: u16,
        column_family_id: u8,
        key: &[u8],
    ) -> Result<()> {
        self.db
            .db()
            .advance_truncation_cursor_by_id(bucket, column_family_id, key)
    }

    pub(crate) fn column_family_truncation_cursor_by_id(
        &self,
        bucket: u16,
        column_family_id: u8,
    ) -> Result<Option<Vec<u8>>> {
        self.db
            .db()
            .truncation_cursor_by_id(bucket, column_family_id)
    }
}

impl StructuredSchemaOwner for StructuredSingleDb {
    fn current_structured_schema(&self) -> StructuredSchema {
        self.current_schema()
    }

    fn begin_core_schema_update(&self) -> cobble::SchemaBuilder {
        self.db.db().update_schema()
    }

    fn reload_structured_schema_from_core(&mut self) -> Result<StructuredSchema> {
        self.reload_schema()?;
        Ok(self.current_schema())
    }
}

#[cfg(test)]
#[path = "../tests/unit/structured_single_db.rs"]
mod tests;
