use crate::structured_db::{
    StructuredColumnValue, StructuredDb, StructuredScanOptions, StructuredWriteOptions,
};
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
enum PriorityQueueBackend<'a> {
    StructuredDb(&'a StructuredDb),
    StructuredSingleDb(&'a StructuredSingleDb),
}

pub struct PriorityQueue<'a> {
    backend: PriorityQueueBackend<'a>,
    column_family: String,
    column_family_id: u8,
    write_options: StructuredWriteOptions,
    scan_options: StructuredScanOptions,
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
            column_family_id,
            write_options: StructuredWriteOptions::with_column_family(column_family.clone()),
            scan_options: StructuredScanOptions::for_column(0)
                .with_column_family(column_family.clone())
                .with_preload_scan_cursor_block(true),
            column_family,
        }
    }

    pub fn column_family(&self) -> &str {
        &self.column_family
    }

    /// Upserts one queue key with merge semantics.
    pub fn offer<K, V>(&self, bucket: u16, key: K, value: V) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        match self.backend {
            PriorityQueueBackend::StructuredDb(db) => db.merge_with_options(
                bucket,
                key.as_ref(),
                0,
                StructuredColumnValue::Bytes(Bytes::copy_from_slice(value.as_ref())),
                &self.write_options,
            ),
            PriorityQueueBackend::StructuredSingleDb(db) => db.merge_with_options(
                bucket,
                key.as_ref(),
                0,
                StructuredColumnValue::Bytes(Bytes::copy_from_slice(value.as_ref())),
                &self.write_options,
            ),
        }
    }

    /// Deletes one queue key if it exists.
    pub fn delete<K>(&self, bucket: u16, key: K) -> Result<()>
    where
        K: AsRef<[u8]>,
    {
        match self.backend {
            PriorityQueueBackend::StructuredDb(db) => {
                db.delete_with_options(bucket, key.as_ref(), 0, &self.write_options)
            }
            PriorityQueueBackend::StructuredSingleDb(db) => {
                db.delete_with_options(bucket, key.as_ref(), 0, &self.write_options)
            }
        }
    }

    /// Returns and removes the smallest key in the queue.
    pub fn poll(&self, bucket: u16) -> Result<Option<(Bytes, Bytes)>> {
        Ok(self.scan_batch(bucket, Some(1), true)?.into_iter().next())
    }

    /// Returns the smallest key in the queue without advancing the queue cursor.
    pub fn peek(&self, bucket: u16) -> Result<Option<(Bytes, Bytes)>> {
        Ok(self.scan_batch(bucket, Some(1), false)?.into_iter().next())
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
        self.scan_batch(bucket, batch_size, true)
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
        self.scan_batch(bucket, batch_size, false)
    }

    /// Advances the queue cursor to `key`, making `key` and earlier items invisible.
    ///
    /// This is a monotonic-consumption API. Items subsequently offered at or
    /// before the cursor remain invisible.
    pub fn advance_to<K>(&self, bucket: u16, key: K) -> Result<()>
    where
        K: AsRef<[u8]>,
    {
        self.advance_cursor_by_id(bucket, key.as_ref())
    }

    /// Returns the current queue cursor, if this bucket has consumed any items.
    pub fn cursor(&self, bucket: u16) -> Result<Option<Vec<u8>>> {
        match self.backend {
            PriorityQueueBackend::StructuredDb(db) => {
                db.column_family_truncation_cursor_by_id(bucket, self.column_family_id)
            }
            PriorityQueueBackend::StructuredSingleDb(db) => {
                db.column_family_truncation_cursor_by_id(bucket, self.column_family_id)
            }
        }
    }

    fn scan_batch(
        &self,
        bucket: u16,
        batch_size: Option<usize>,
        advance_cursor: bool,
    ) -> Result<Vec<(Bytes, Bytes)>> {
        if batch_size == Some(0) {
            return Ok(Vec::new());
        }
        let scan_options = if batch_size.is_none() {
            self.scan_options.clone().with_stop_at_block_boundary(true)
        } else {
            self.scan_options.clone()
        };
        let mut iter = self.scan_raw_bounds(bucket, &scan_options)?;
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
            let (key, columns) = row?;
            let value = columns.into_iter().next().flatten().ok_or_else(|| {
                Error::IoError(
                    "priority queue scan returned no value for projected column".to_string(),
                )
            })?;
            let value = Bytes::copy_from_slice(value.as_ref());
            last_key = Some(key.clone());
            rows.push((key, value));
        }
        let Some(last_key) = last_key else {
            return Ok(rows);
        };
        if advance_cursor {
            self.advance_cursor_by_id(bucket, last_key.as_ref())?;
        }
        Ok(rows)
    }

    fn scan_raw_bounds(
        &self,
        bucket: u16,
        scan_options: &StructuredScanOptions,
    ) -> Result<DbIterator> {
        match self.backend {
            PriorityQueueBackend::StructuredDb(db) => {
                db.scan_raw_bounds(bucket, None, None, scan_options)
            }
            PriorityQueueBackend::StructuredSingleDb(db) => {
                db.scan_raw_bounds(bucket, None, None, scan_options)
            }
        }
    }

    fn advance_cursor_by_id(&self, bucket: u16, key: &[u8]) -> Result<()> {
        match self.backend {
            PriorityQueueBackend::StructuredDb(db) => {
                db.advance_column_family_truncation_cursor_by_id(bucket, self.column_family_id, key)
            }
            PriorityQueueBackend::StructuredSingleDb(db) => {
                db.advance_column_family_truncation_cursor_by_id(bucket, self.column_family_id, key)
            }
        }
    }
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
