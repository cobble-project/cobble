use crate::structured_db::{
    StructuredColumnValue, StructuredDbIterator, StructuredSchema, StructuredSchemaBuilder,
    StructuredSchemaOwner, StructuredWriteBatch, decode_row, encode_for_write,
    load_structured_schema_from_cobble_schema, persist_structured_schema_on_db,
};
use cobble::{Config, ReadOptions, Result, ScanOptions, SingleDb, WriteOptions};
use std::ops::Range;
use std::sync::Arc;

pub struct StructuredSingleDb {
    db: SingleDb,
    structured_schema: Arc<StructuredSchema>,
}

impl StructuredSingleDb {
    pub fn open(config: Config) -> Result<Self> {
        let db = SingleDb::open(config)?;
        let structured_schema =
            load_structured_schema_from_cobble_schema(&db.db().current_schema())?;
        Ok(Self {
            db,
            structured_schema: Arc::new(structured_schema),
        })
    }

    pub fn db(&self) -> &SingleDb {
        &self.db
    }

    pub fn current_schema(&self) -> StructuredSchema {
        self.structured_schema.as_ref().clone()
    }

    pub fn update_schema(&mut self) -> StructuredSchemaBuilder<'_, Self> {
        StructuredSchemaBuilder::new(self)
    }
}

impl StructuredSingleDb {
    pub fn reload_schema(&mut self) -> Result<()> {
        let schema = load_structured_schema_from_cobble_schema(&self.db.db().current_schema())?;
        self.structured_schema = Arc::new(schema);
        Ok(())
    }

    pub fn apply_schema(
        &mut self,
        structured_schema: StructuredSchema,
    ) -> Result<StructuredSchema> {
        persist_structured_schema_on_db(self.db.db(), &structured_schema)?;
        let reloaded = load_structured_schema_from_cobble_schema(&self.db.db().current_schema())?;
        self.structured_schema = Arc::new(reloaded.clone());
        Ok(reloaded)
    }

    // ── Write operations ────────────────────────────────────────────────

    pub fn put<K, V>(&self, bucket: u16, key: K, column: u16, value: V) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: Into<StructuredColumnValue>,
    {
        self.put_with_options(bucket, key, column, value, &WriteOptions::default())
    }

    pub fn put_with_options<K, V>(
        &self,
        bucket: u16,
        key: K,
        column: u16,
        value: V,
        options: &WriteOptions,
    ) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: Into<StructuredColumnValue>,
    {
        let encoded = encode_for_write(
            &self.structured_schema,
            options.column_family.as_deref(),
            self.db.db().now_seconds(),
            column,
            value.into(),
            options.ttl_seconds,
        )?;
        self.db
            .put_with_options(bucket, key, column, encoded, options)
    }

    pub fn merge<K, V>(&self, bucket: u16, key: K, column: u16, value: V) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: Into<StructuredColumnValue>,
    {
        self.merge_with_options(bucket, key, column, value, &WriteOptions::default())
    }

    pub fn merge_with_options<K, V>(
        &self,
        bucket: u16,
        key: K,
        column: u16,
        value: V,
        options: &WriteOptions,
    ) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: Into<StructuredColumnValue>,
    {
        let encoded = encode_for_write(
            &self.structured_schema,
            options.column_family.as_deref(),
            self.db.db().now_seconds(),
            column,
            value.into(),
            options.ttl_seconds,
        )?;
        self.db
            .merge_with_options(bucket, key, column, encoded, options)
    }

    pub fn delete<K>(&self, bucket: u16, key: K, column: u16) -> Result<()>
    where
        K: AsRef<[u8]>,
    {
        self.delete_with_options(bucket, key, column, &WriteOptions::default())
    }

    pub fn delete_with_options<K>(
        &self,
        bucket: u16,
        key: K,
        column: u16,
        options: &WriteOptions,
    ) -> Result<()>
    where
        K: AsRef<[u8]>,
    {
        self.db.delete_with_options(bucket, key, column, options)
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

    // ── Read operations ─────────────────────────────────────────────────

    pub fn get<K>(&self, bucket: u16, key: K) -> Result<Option<Vec<Option<StructuredColumnValue>>>>
    where
        K: AsRef<[u8]>,
    {
        let raw = self.db.get(bucket, key.as_ref())?;
        let default_schema = self.structured_schema.projected(0, None);
        raw.map(|columns| decode_row(&default_schema, 0, columns))
            .transpose()
    }

    pub fn get_with_options<K>(
        &self,
        bucket: u16,
        key: K,
        options: &ReadOptions,
    ) -> Result<Option<Vec<Option<StructuredColumnValue>>>>
    where
        K: AsRef<[u8]>,
    {
        let raw = self.db.get_with_options(bucket, key.as_ref(), options)?;
        let projected_schema = self.structured_schema.project_structured_family(
            options.column_family.as_deref(),
            options.column_indices.as_deref(),
        )?;
        raw.map(|columns| decode_row(&projected_schema, 0, columns))
            .transpose()
    }

    pub fn scan<'a>(
        &'a self,
        bucket: u16,
        range: Range<&[u8]>,
    ) -> Result<StructuredDbIterator<'a>> {
        self.scan_with_options(bucket, range, &ScanOptions::default())
    }

    pub fn scan_with_options<'a>(
        &'a self,
        bucket: u16,
        range: Range<&[u8]>,
        options: &ScanOptions,
    ) -> Result<StructuredDbIterator<'a>> {
        let inner = self.db.scan_with_options(bucket, range, options)?;
        let projected_schema = self.structured_schema.project_structured_family(
            options.column_family.as_deref(),
            options.column_indices.as_deref(),
        )?;
        Ok(StructuredDbIterator::new(inner, projected_schema, 0))
    }

    // ── Snapshot lifecycle ───────────────────────────────────────────────

    pub fn snapshot(&self) -> Result<u64> {
        self.db.snapshot()
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
mod tests {
    use super::*;
    use crate::list::{ListConfig, ListRetainMode};
    use bytes::Bytes;
    use cobble::{ReadOptions, VolumeDescriptor};
    use std::thread;
    use std::time::Duration;
    use uuid::Uuid;

    fn apply_test_schema(db: &mut StructuredSingleDb) {
        db.update_schema()
            .add_list_column(
                None,
                1,
                ListConfig {
                    max_elements: Some(3),
                    retain_mode: ListRetainMode::Last,
                    preserve_element_ttl: false,
                },
            )
            .commit()
            .unwrap();
    }

    fn test_config(root: &str) -> Config {
        Config {
            volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
            num_columns: 1,
            total_buckets: 2,
            snapshot_on_flush: true,
            ..Config::default()
        }
    }

    #[test]
    fn test_structured_single_db_put_get_scan() {
        let root = format!("/tmp/ds_single_put_get_{}", Uuid::new_v4());
        let mut db = StructuredSingleDb::open(test_config(&root)).unwrap();
        apply_test_schema(&mut db);

        db.put(0, b"k1", 0, Bytes::from_static(b"v0")).unwrap();
        db.merge(0, b"k1", 1, vec![Bytes::from_static(b"a")])
            .unwrap();
        db.merge(0, b"k1", 1, vec![Bytes::from_static(b"b")])
            .unwrap();

        let row = db.get(0, b"k1").unwrap().expect("row exists");
        assert_eq!(
            row[0],
            Some(StructuredColumnValue::Bytes(Bytes::from_static(b"v0")))
        );
        assert_eq!(
            row[1],
            Some(StructuredColumnValue::List(vec![
                Bytes::from_static(b"a"),
                Bytes::from_static(b"b"),
            ]))
        );

        let mut iter = db.scan(0, b"k0".as_ref()..b"k9".as_ref()).unwrap();
        let first = iter.next().expect("one row").unwrap();
        assert_eq!(first.0.as_ref(), b"k1");
        assert!(iter.next().is_none());
        drop(iter);

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn test_structured_single_db_write_batch() {
        let root = format!("/tmp/ds_single_batch_{}", Uuid::new_v4());
        let mut db = StructuredSingleDb::open(test_config(&root)).unwrap();
        apply_test_schema(&mut db);

        let mut batch = db.new_write_batch();
        batch.put(0, b"k1", 0, Bytes::from_static(b"v0")).unwrap();
        batch
            .merge(0, b"k1", 1, vec![Bytes::from_static(b"a")])
            .unwrap();
        batch
            .merge(0, b"k1", 1, vec![Bytes::from_static(b"b")])
            .unwrap();
        db.write_batch(batch).unwrap();

        let row = db.get(0, b"k1").unwrap().expect("row exists");
        assert_eq!(
            row[1],
            Some(StructuredColumnValue::List(vec![
                Bytes::from_static(b"a"),
                Bytes::from_static(b"b"),
            ]))
        );

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn test_structured_single_db_delete() {
        let root = format!("/tmp/ds_single_delete_{}", Uuid::new_v4());
        let mut db = StructuredSingleDb::open(test_config(&root)).unwrap();
        apply_test_schema(&mut db);

        db.put(0, b"k1", 0, Bytes::from_static(b"v0")).unwrap();
        assert!(db.get(0, b"k1").unwrap().is_some());
        db.delete(0, b"k1", 0).unwrap();
        // After deleting column 0, the row may still be present but column 0 is None
        let row = db.get(0, b"k1").unwrap();
        if let Some(row) = row {
            assert_eq!(row[0], None);
        }

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn test_structured_single_db_snapshot_lifecycle() {
        let root = format!("/tmp/ds_single_snap_{}", Uuid::new_v4());
        let mut db = StructuredSingleDb::open(test_config(&root)).unwrap();
        apply_test_schema(&mut db);

        db.put(0, b"k1", 0, Bytes::from_static(b"v0")).unwrap();
        let snap_id = db.snapshot().unwrap();
        // Snapshot ID is allocated from 0, just check it succeeds
        thread::sleep(Duration::from_millis(300));

        let snapshots = db.list_snapshots().unwrap();
        assert!(!snapshots.is_empty());
        assert_eq!(snapshots[0].id, snap_id);

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn test_structured_single_db_get_with_projection_reindexes_schema() {
        let root = format!("/tmp/ds_single_get_projection_{}", Uuid::new_v4());
        let mut db = StructuredSingleDb::open(test_config(&root)).unwrap();
        apply_test_schema(&mut db);
        db.put(0, b"k1", 0, Bytes::from_static(b"v0")).unwrap();
        db.merge(0, b"k1", 1, vec![Bytes::from_static(b"a")])
            .unwrap();
        db.merge(0, b"k1", 1, vec![Bytes::from_static(b"b")])
            .unwrap();

        let row = db
            .get_with_options(0, b"k1", &ReadOptions::for_column(1))
            .unwrap()
            .expect("row exists");
        assert_eq!(row.len(), 1);
        assert_eq!(
            row[0],
            Some(StructuredColumnValue::List(vec![
                Bytes::from_static(b"a"),
                Bytes::from_static(b"b"),
            ]))
        );

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(root);
    }
}
