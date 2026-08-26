use crate::structured_db::{StructuredDb, StructuredScanOptions, StructuredWriteOptions};
use crate::structured_single_db::StructuredSingleDb;
use bytes::Bytes;
use cobble::{ColumnFamilyOptions, DbIterator, Error, Result, Schema};
use serde_json::{Value as JsonValue, json};

const PRIORITY_QUEUE_FAMILY_KIND: &str = "priority_queue";
const DEFAULT_COLUMN_FAMILY_NAME: &str = "default";

/// Column-family-scoped priority queue built on top of structured DB operations.
///
/// Each queue owns one dedicated column family. Queue operations map directly onto structured
/// merge/delete/scan calls in that family. This type is the abstraction boundary for the embedded
/// and sharded structured backends: callers above `cobble-data-structure` get one queue API, while
/// this layer chooses the appropriate `StructuredDb` or `StructuredSingleDb` implementation.
#[derive(Clone, Copy)]
pub(crate) enum PriorityQueueBackend<'a> {
    StructuredDb(&'a StructuredDb),
    StructuredSingleDb(&'a StructuredSingleDb),
}

pub struct PriorityQueue<'a> {
    backend: PriorityQueueBackend<'a>,
    descriptor: DetachedPriorityQueueDescriptor,
}

/// Owned, backend-independent queue metadata used by language bindings.
///
/// This deliberately contains no borrow of a database. Schema validation and
/// option construction happen once when the descriptor is created; callers
/// supply a concrete backend only for the duration of an operation.
#[derive(Clone)]
pub(crate) struct DetachedPriorityQueueDescriptor {
    column_family: String,
    column_family_id: u8,
    write_options: StructuredWriteOptions,
    fixed_scan_options: StructuredScanOptions,
    boundary_scan_options: StructuredScanOptions,
}

impl<'a> PriorityQueue<'a> {
    pub(crate) fn from_column_family(
        db: &'a StructuredDb,
        column_family: String,
        column_family_id: u8,
    ) -> Self {
        Self::from_backend(
            PriorityQueueBackend::StructuredDb(db),
            column_family,
            column_family_id,
        )
    }

    pub(crate) fn from_single_column_family(
        db: &'a StructuredSingleDb,
        column_family: String,
        column_family_id: u8,
    ) -> Self {
        Self::from_backend(
            PriorityQueueBackend::StructuredSingleDb(db),
            column_family,
            column_family_id,
        )
    }

    fn from_backend(
        backend: PriorityQueueBackend<'a>,
        column_family: String,
        column_family_id: u8,
    ) -> Self {
        Self {
            backend,
            descriptor: DetachedPriorityQueueDescriptor::new(column_family, column_family_id),
        }
    }

    pub fn column_family(&self) -> &str {
        self.descriptor.column_family()
    }

    /// Upserts one queue key with merge semantics.
    pub fn offer<K, V>(&self, bucket: u16, key: K, value: V) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.descriptor
            .offer(self.backend, bucket, key.as_ref(), value.as_ref())
    }

    /// Deletes one queue key if it exists.
    pub fn delete<K>(&self, bucket: u16, key: K) -> Result<()>
    where
        K: AsRef<[u8]>,
    {
        self.descriptor.delete(self.backend, bucket, key.as_ref())
    }

    /// Returns and removes the smallest key in the queue.
    pub fn poll(&self, bucket: u16) -> Result<Option<(Bytes, Bytes)>> {
        Ok(self
            .descriptor
            .scan_batch(self.backend, bucket, Some(1), true)?
            .into_iter()
            .next())
    }

    /// Returns the smallest key in the queue without advancing the queue cursor.
    pub fn peek(&self, bucket: u16) -> Result<Option<(Bytes, Bytes)>> {
        Ok(self
            .descriptor
            .scan_batch(self.backend, bucket, Some(1), false)?
            .into_iter()
            .next())
    }

    /// Returns and removes a batch of the smallest keys in the queue.
    ///
    /// When `batch_size` is `Some(n)`, up to `n` rows are returned. When it is
    /// `None`, the scan asks the underlying iterators to stop at the next
    /// physical boundary: SST data block, Parquet row group, or file boundary.
    /// Sources without physical boundary semantics keep producing rows normally.
    /// In all cases the column-family truncation cursor is advanced at most once,
    /// to the last returned key.
    pub fn poll_batch(
        &self,
        bucket: u16,
        batch_size: Option<usize>,
    ) -> Result<Vec<(Bytes, Bytes)>> {
        self.descriptor
            .scan_batch(self.backend, bucket, batch_size, true)
    }

    /// Returns a batch of the smallest keys in the queue without advancing the queue cursor.
    ///
    /// When `batch_size` is `Some(n)`, up to `n` rows are returned. When it is
    /// `None`, the scan asks the underlying iterators to stop at the next
    /// physical boundary: SST data block, Parquet row group, or file boundary.
    /// Sources without physical boundary semantics keep producing rows normally.
    pub fn peek_batch(
        &self,
        bucket: u16,
        batch_size: Option<usize>,
    ) -> Result<Vec<(Bytes, Bytes)>> {
        self.descriptor
            .scan_batch(self.backend, bucket, batch_size, false)
    }

    /// Advances the queue cursor to `key`, making `key` and earlier items invisible.
    ///
    /// This is a monotonic-consumption API. Items subsequently offered at or
    /// before the cursor remain invisible.
    pub fn advance_to<K>(&self, bucket: u16, key: K) -> Result<()>
    where
        K: AsRef<[u8]>,
    {
        self.descriptor
            .advance_cursor(self.backend, bucket, key.as_ref())
    }

    /// Returns the current queue cursor, if this bucket has consumed any items.
    pub fn cursor(&self, bucket: u16) -> Result<Option<Vec<u8>>> {
        self.descriptor.cursor(self.backend, bucket)
    }

    #[cfg(feature = "ffi")]
    pub(crate) fn detached_descriptor(&self) -> DetachedPriorityQueueDescriptor {
        self.descriptor.clone()
    }
}

impl DetachedPriorityQueueDescriptor {
    fn new(column_family: String, column_family_id: u8) -> Self {
        let fixed_scan_options = StructuredScanOptions::for_column(0)
            .with_column_family(column_family.clone())
            .with_preload_scan_cursor_block(true);
        let boundary_scan_options = fixed_scan_options.clone().with_stop_at_block_boundary(true);
        Self {
            write_options: StructuredWriteOptions::with_column_family(column_family.clone()),
            fixed_scan_options,
            boundary_scan_options,
            column_family,
            column_family_id,
        }
    }

    pub(crate) fn column_family(&self) -> &str {
        &self.column_family
    }

    pub(crate) fn offer(
        &self,
        backend: PriorityQueueBackend<'_>,
        bucket: u16,
        key: &[u8],
        value: &[u8],
    ) -> Result<()> {
        match backend {
            PriorityQueueBackend::StructuredDb(db) => {
                db.merge_borrowed_bytes_with_options(bucket, key, 0, value, &self.write_options)
            }
            PriorityQueueBackend::StructuredSingleDb(db) => {
                db.merge_borrowed_bytes_with_options(bucket, key, 0, value, &self.write_options)
            }
        }
    }

    pub(crate) fn delete(
        &self,
        backend: PriorityQueueBackend<'_>,
        bucket: u16,
        key: &[u8],
    ) -> Result<()> {
        match backend {
            PriorityQueueBackend::StructuredDb(db) => {
                db.delete_with_options(bucket, key, 0, &self.write_options)
            }
            PriorityQueueBackend::StructuredSingleDb(db) => {
                db.delete_with_options(bucket, key, 0, &self.write_options)
            }
        }
    }

    pub(crate) fn scan_batch(
        &self,
        backend: PriorityQueueBackend<'_>,
        bucket: u16,
        batch_size: Option<usize>,
        advance_cursor: bool,
    ) -> Result<Vec<(Bytes, Bytes)>> {
        if batch_size == Some(0) {
            return Ok(Vec::new());
        }
        let scan_options = if batch_size.is_none() {
            &self.boundary_scan_options
        } else {
            &self.fixed_scan_options
        };
        let mut iter = self.scan_raw_bounds(backend, bucket, scan_options)?;
        let mut rows = Vec::with_capacity(batch_size.unwrap_or(1));
        let mut last_key = None;
        while batch_size.is_none_or(|limit| rows.len() < limit) {
            let Some(row) = iter.next() else {
                if batch_size.is_none() && rows.is_empty() && iter.stopped_at_block_boundary() {
                    iter.clear_stop_at_block_boundary();
                    continue;
                }
                break;
            };
            let (key, value) = decode_priority_queue_row(row?)?;
            last_key = Some(key.clone());
            rows.push((key, value));
        }
        let Some(last_key) = last_key else {
            return Ok(rows);
        };
        if advance_cursor {
            self.advance_cursor(backend, bucket, last_key.as_ref())?;
        }
        Ok(rows)
    }

    fn scan_raw_bounds(
        &self,
        backend: PriorityQueueBackend<'_>,
        bucket: u16,
        scan_options: &StructuredScanOptions,
    ) -> Result<DbIterator> {
        match backend {
            PriorityQueueBackend::StructuredDb(db) => {
                db.scan_raw_bounds(bucket, None, None, scan_options)
            }
            PriorityQueueBackend::StructuredSingleDb(db) => {
                db.scan_raw_bounds(bucket, None, None, scan_options)
            }
        }
    }

    pub(crate) fn advance_cursor(
        &self,
        backend: PriorityQueueBackend<'_>,
        bucket: u16,
        key: &[u8],
    ) -> Result<()> {
        match backend {
            PriorityQueueBackend::StructuredDb(db) => {
                db.advance_column_family_truncation_cursor_by_id(bucket, self.column_family_id, key)
            }
            PriorityQueueBackend::StructuredSingleDb(db) => {
                db.advance_column_family_truncation_cursor_by_id(bucket, self.column_family_id, key)
            }
        }
    }

    pub(crate) fn cursor(
        &self,
        backend: PriorityQueueBackend<'_>,
        bucket: u16,
    ) -> Result<Option<Vec<u8>>> {
        match backend {
            PriorityQueueBackend::StructuredDb(db) => {
                db.column_family_truncation_cursor_by_id(bucket, self.column_family_id)
            }
            PriorityQueueBackend::StructuredSingleDb(db) => {
                db.column_family_truncation_cursor_by_id(bucket, self.column_family_id)
            }
        }
    }
}

fn decode_priority_queue_row(
    (key, columns): (Bytes, Vec<Option<Bytes>>),
) -> Result<(Bytes, Bytes)> {
    let mut columns = columns.into_iter();
    let value = columns.next().flatten().ok_or_else(|| {
        Error::IoError(
            "priority queue scan returned no value for projected BYTES column".to_string(),
        )
    })?;
    if columns.next().is_some() {
        return Err(Error::IoError(
            "priority queue scan returned an unexpected projected column layout".to_string(),
        ));
    }
    Ok((key, value))
}

pub(crate) fn priority_queue_column_family_options() -> ColumnFamilyOptions {
    ColumnFamilyOptions {
        metadata: Some(priority_queue_column_family_metadata()),
        ..ColumnFamilyOptions::default()
    }
}

pub(crate) fn priority_queue_column_family_name(name: String) -> Result<String> {
    let normalized = name.trim().to_string();
    if normalized.is_empty() {
        return Err(Error::InvalidState(
            "column family name cannot be empty".to_string(),
        ));
    }
    if normalized == DEFAULT_COLUMN_FAMILY_NAME {
        return Err(Error::InputError(
            "priority queue cannot use the default column family".to_string(),
        ));
    }
    Ok(normalized)
}

pub(crate) fn validate_priority_queue_column_family(
    schema: &Schema,
    column_family: &str,
) -> Result<u8> {
    let column_family_id = schema
        .column_family_ids()
        .get(column_family)
        .copied()
        .ok_or_else(|| {
            Error::InvalidState(format!(
                "unknown priority queue column family '{}'",
                column_family
            ))
        })?;
    let options = schema.column_family_options_in_family(column_family_id);
    let Some(metadata) = options.metadata.as_ref() else {
        return Err(Error::InvalidState(format!(
            "column family '{}' is not marked as a priority queue",
            column_family
        )));
    };
    if priority_queue_family_kind(metadata) != Some(PRIORITY_QUEUE_FAMILY_KIND) {
        return Err(Error::InvalidState(format!(
            "column family '{}' is not marked as a priority queue",
            column_family
        )));
    }
    let num_columns = schema
        .column_families()
        .into_iter()
        .find(|(name, _)| name == column_family)
        .map(|(_, num_columns)| num_columns);
    if num_columns != Some(1) {
        return Err(Error::InvalidState(format!(
            "priority queue column family '{}' must have exactly one column",
            column_family
        )));
    }
    Ok(column_family_id)
}

fn priority_queue_column_family_metadata() -> JsonValue {
    json!({
        "kind": PRIORITY_QUEUE_FAMILY_KIND,
    })
}

fn priority_queue_family_kind(metadata: &JsonValue) -> Option<&str> {
    metadata.get("kind").and_then(JsonValue::as_str)
}

#[cfg(test)]
#[path = "../tests/unit/priority_queue.rs"]
mod tests;
