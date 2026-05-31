use crate::structured_db::{
    StructuredColumnValue, StructuredDb, StructuredScanOptions, StructuredWriteOptions,
};
use bytes::Bytes;
use cobble::{ColumnFamilyOptions, Error, Result, Schema};
use serde_json::{Value as JsonValue, json};

const PRIORITY_QUEUE_FAMILY_KIND: &str = "priority_queue";

/// Column-family-scoped priority queue built on top of existing `StructuredDb` operations.
///
/// Each queue owns one dedicated column family. Queue operations map directly onto structured
/// merge/delete/scan calls in that family.
pub struct PriorityQueue<'a> {
    db: &'a StructuredDb,
    column_family: String,
    write_options: StructuredWriteOptions,
    scan_options: StructuredScanOptions,
}

impl<'a> PriorityQueue<'a> {
    pub(crate) fn from_column_family(db: &'a StructuredDb, column_family: String) -> Self {
        Self {
            db,
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
        self.db.merge_with_options(
            bucket,
            key.as_ref(),
            0,
            StructuredColumnValue::Bytes(Bytes::copy_from_slice(value.as_ref())),
            &self.write_options,
        )
    }

    /// Deletes one queue key if it exists.
    pub fn delete<K>(&self, bucket: u16, key: K) -> Result<()>
    where
        K: AsRef<[u8]>,
    {
        self.db
            .delete_with_options(bucket, key.as_ref(), 0, &self.write_options)
    }

    /// Returns and removes the smallest key in the queue.
    pub fn poll(&self, bucket: u16) -> Result<Option<(Bytes, Bytes)>> {
        Ok(self.poll_batch(bucket, Some(1))?.into_iter().next())
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
        if batch_size == Some(0) {
            return Ok(Vec::new());
        }
        let scan_options = if batch_size.is_none() {
            self.scan_options.clone().with_stop_at_block_boundary(true)
        } else {
            self.scan_options.clone()
        };
        let mut iter = self.db.scan_raw_bounds(bucket, None, None, &scan_options)?;
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
                    "priority queue poll returned no value for projected column".to_string(),
                )
            })?;
            let value = Bytes::copy_from_slice(value.as_ref());
            last_key = Some(key.clone());
            rows.push((key, value));
        }
        let Some(last_key) = last_key else {
            return Ok(rows);
        };
        self.db.advance_column_family_truncation_cursor(
            bucket,
            self.column_family.as_str(),
            last_key.as_ref(),
        )?;
        Ok(rows)
    }
}

pub(crate) fn priority_queue_column_family_options() -> ColumnFamilyOptions {
    ColumnFamilyOptions {
        metadata: Some(priority_queue_column_family_metadata()),
        ..ColumnFamilyOptions::default()
    }
}

pub(crate) fn validate_priority_queue_column_family(
    schema: &Schema,
    column_family: &str,
) -> Result<()> {
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
    Ok(())
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
mod tests {
    use crate::{StructuredDb, StructuredReadOptions};
    use bytes::Bytes;
    use cobble::{Config, Error, Result, VolumeDescriptor};
    use std::thread;
    use std::time::Duration;
    use uuid::Uuid;

    fn open_test_db(root: &str) -> Result<StructuredDb> {
        StructuredDb::open(
            Config {
                volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
                num_columns: 1,
                snapshot_on_flush: true,
                ..Config::default()
            },
            vec![0u16..=0u16],
        )
    }

    #[test]
    fn test_priority_queue_offer_merge_and_poll_order() {
        let root = format!("/tmp/ds_priority_queue_order_{}", Uuid::new_v4());
        let mut db = open_test_db(&root).unwrap();
        db.new_priority_queue("jobs").unwrap();
        let queue = db.get_priority_queue("jobs").unwrap();
        assert!(queue.scan_options.as_cobble().preload_scan_cursor_block());

        queue.offer(0, b"k2", b"v2").unwrap();
        queue.offer(0, b"k1", b"left").unwrap();
        queue.offer(0, b"k1", b"right").unwrap();

        let first = queue.poll(0).unwrap().expect("first poll");
        assert_eq!(first.0.as_ref(), b"k1");
        assert_eq!(first.1.as_ref(), b"leftright");

        let second = queue.poll(0).unwrap().expect("second poll");
        assert_eq!(second.0.as_ref(), b"k2");
        assert_eq!(second.1.as_ref(), b"v2");

        assert!(queue.poll(0).unwrap().is_none());
        db.close().unwrap();
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn test_priority_queue_delete_and_namespace_isolation() {
        let root = format!("/tmp/ds_priority_queue_delete_{}", Uuid::new_v4());
        let mut db = open_test_db(&root).unwrap();
        db.new_priority_queue("jobs-a").unwrap();
        db.new_priority_queue("jobs-b").unwrap();

        let queue_a = db.get_priority_queue("jobs-a").unwrap();
        let queue_b = db.get_priority_queue("jobs-b").unwrap();
        queue_a.offer(0, b"shared", b"va").unwrap();
        queue_b.offer(0, b"shared", b"vb").unwrap();
        queue_a.delete(0, b"missing").unwrap();
        queue_a.delete(0, b"shared").unwrap();
        assert!(queue_a.poll(0).unwrap().is_none());

        let from_b = queue_b.poll(0).unwrap().expect("poll jobs-b");
        assert_eq!(from_b.0.as_ref(), b"shared");
        assert_eq!(from_b.1.as_ref(), b"vb");

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn test_priority_queue_poll_after_resume() {
        let root = format!("/tmp/ds_priority_queue_resume_{}", Uuid::new_v4());
        let config = Config {
            volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
            num_columns: 1,
            snapshot_on_flush: true,
            ..Config::default()
        };
        let db_id = {
            let mut db = StructuredDb::open(config.clone(), vec![0u16..=0u16]).unwrap();
            let queue = db.get_or_new_priority_queue("jobs").unwrap();
            queue.offer(0, b"k2", b"v2").unwrap();
            queue.offer(0, b"k1", b"v1").unwrap();
            let _ = db.snapshot().unwrap();
            thread::sleep(Duration::from_millis(250));
            let db_id = db.id().to_string();
            db.close().unwrap();
            db_id
        };

        let resumed = StructuredDb::resume(config, db_id).unwrap();
        let queue = resumed.get_priority_queue("jobs").unwrap();
        let first = queue.poll(0).unwrap().expect("first poll");
        assert_eq!(
            first,
            (Bytes::from_static(b"k1"), Bytes::from_static(b"v1"))
        );
        let second = queue.poll(0).unwrap().expect("second poll");
        assert_eq!(
            second,
            (Bytes::from_static(b"k2"), Bytes::from_static(b"v2"))
        );
        resumed.close().unwrap();
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn test_priority_queue_poll_persists_truncation_cursor() {
        let root = format!("/tmp/ds_priority_queue_truncation_{}", Uuid::new_v4());
        let config = Config {
            volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
            num_columns: 1,
            snapshot_on_flush: true,
            ..Config::default()
        };
        let db_id = {
            let mut db = StructuredDb::open(config.clone(), vec![0u16..=0u16]).unwrap();
            let queue = db.get_or_new_priority_queue("jobs").unwrap();
            queue.offer(0, b"k1", b"v1").unwrap();
            queue.offer(0, b"k2", b"v2").unwrap();

            let first = queue.poll(0).unwrap().expect("first poll");
            assert_eq!(
                first,
                (Bytes::from_static(b"k1"), Bytes::from_static(b"v1"))
            );
            let read_options = StructuredReadOptions::for_column(0).with_column_family("jobs");
            assert!(
                db.get_raw_with_options(0, b"k1", &read_options)
                    .unwrap()
                    .is_none()
            );
            assert!(
                db.get_raw_with_options(0, b"k2", &read_options)
                    .unwrap()
                    .is_some()
            );

            let _ = db.snapshot().unwrap();
            thread::sleep(Duration::from_millis(250));
            let db_id = db.id().to_string();
            db.close().unwrap();
            db_id
        };

        let resumed = StructuredDb::resume(config, db_id).unwrap();
        let queue = resumed.get_priority_queue("jobs").unwrap();
        let remaining = queue.poll(0).unwrap().expect("remaining poll");
        assert_eq!(
            remaining,
            (Bytes::from_static(b"k2"), Bytes::from_static(b"v2"))
        );
        assert!(queue.poll(0).unwrap().is_none());
        resumed.close().unwrap();
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn test_priority_queue_poll_batch_advances_cursor_once_to_last_key() {
        let root = format!("/tmp/ds_priority_queue_batch_{}", Uuid::new_v4());
        let mut db = open_test_db(&root).unwrap();
        db.new_priority_queue("jobs").unwrap();
        let queue = db.get_priority_queue("jobs").unwrap();
        queue.offer(0, b"k1", b"v1").unwrap();
        queue.offer(0, b"k2", b"v2").unwrap();
        queue.offer(0, b"k3", b"v3").unwrap();

        let batch = queue.poll_batch(0, Some(2)).unwrap();
        assert_eq!(
            batch,
            vec![
                (Bytes::from_static(b"k1"), Bytes::from_static(b"v1")),
                (Bytes::from_static(b"k2"), Bytes::from_static(b"v2")),
            ]
        );
        let read_options = StructuredReadOptions::for_column(0).with_column_family("jobs");
        assert!(
            db.get_raw_with_options(0, b"k1", &read_options)
                .unwrap()
                .is_none()
        );
        assert!(
            db.get_raw_with_options(0, b"k2", &read_options)
                .unwrap()
                .is_none()
        );
        assert!(
            db.get_raw_with_options(0, b"k3", &read_options)
                .unwrap()
                .is_some()
        );

        let remaining = queue.poll_batch(0, Some(10)).unwrap();
        assert_eq!(
            remaining,
            vec![(Bytes::from_static(b"k3"), Bytes::from_static(b"v3"))]
        );
        assert!(queue.poll_batch(0, Some(10)).unwrap().is_empty());
        db.close().unwrap();
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn test_priority_queue_poll_batch_without_size_keeps_unbounded_sources_unlimited() {
        let root = format!("/tmp/ds_priority_queue_dynamic_batch_{}", Uuid::new_v4());
        let mut db = open_test_db(&root).unwrap();
        db.new_priority_queue("jobs").unwrap();
        let queue = db.get_priority_queue("jobs").unwrap();
        queue.offer(0, b"k1", b"v1").unwrap();
        queue.offer(0, b"k2", b"v2").unwrap();

        let batch = queue.poll_batch(0, None).unwrap();
        assert_eq!(
            batch,
            vec![
                (Bytes::from_static(b"k1"), Bytes::from_static(b"v1")),
                (Bytes::from_static(b"k2"), Bytes::from_static(b"v2")),
            ]
        );
        assert!(queue.poll(0).unwrap().is_none());
        db.close().unwrap();
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn test_priority_queue_poll_batch_without_size_reads_one_sst_block() {
        let root = format!(
            "/tmp/ds_priority_queue_dynamic_sst_batch_{}",
            Uuid::new_v4()
        );
        let config = Config {
            volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
            num_columns: 1,
            snapshot_on_flush: true,
            ..Config::default()
        };
        let db_id = {
            let mut db = StructuredDb::open(config.clone(), vec![0u16..=0u16]).unwrap();
            let queue = db.get_or_new_priority_queue("jobs").unwrap();
            queue.offer(0, b"k1", b"v1").unwrap();
            queue.offer(0, b"k2", b"v2").unwrap();
            queue.offer(0, b"k3", b"v3").unwrap();
            let _ = db.snapshot().unwrap();
            thread::sleep(Duration::from_millis(250));
            let db_id = db.id().to_string();
            db.close().unwrap();
            db_id
        };

        let resumed = StructuredDb::resume(config, db_id).unwrap();
        let queue = resumed.get_priority_queue("jobs").unwrap();
        let batch = queue.poll_batch(0, None).unwrap();
        assert_eq!(
            batch,
            vec![
                (Bytes::from_static(b"k1"), Bytes::from_static(b"v1")),
                (Bytes::from_static(b"k2"), Bytes::from_static(b"v2")),
                (Bytes::from_static(b"k3"), Bytes::from_static(b"v3")),
            ]
        );
        assert!(queue.poll(0).unwrap().is_none());
        resumed.close().unwrap();
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn test_new_priority_queue_rejects_non_queue_family() {
        let root = format!("/tmp/ds_priority_queue_reject_family_{}", Uuid::new_v4());
        let mut db = open_test_db(&root).unwrap();
        db.update_schema()
            .add_bytes_column(Some("plain-family".to_string()), 0)
            .commit()
            .unwrap();

        let err = match db.get_or_new_priority_queue("plain-family") {
            Ok(_) => panic!("plain family must not reopen as queue"),
            Err(err) => err,
        };
        assert!(
            matches!(err, Error::InvalidState(message) if message.contains("not marked as a priority queue"))
        );

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn test_new_priority_queue_rejects_existing_queue_family() {
        let root = format!("/tmp/ds_priority_queue_duplicate_family_{}", Uuid::new_v4());
        let mut db = open_test_db(&root).unwrap();
        db.new_priority_queue("jobs").unwrap();

        let err = match db.new_priority_queue("jobs") {
            Ok(_) => panic!("new_priority_queue must fail on existing family"),
            Err(err) => err,
        };
        assert!(matches!(err, Error::InvalidState(message) if message.contains("already exists")));

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(root);
    }
}
