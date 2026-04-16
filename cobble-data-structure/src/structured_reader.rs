use crate::structured_db::{
    StructuredColumnValue, StructuredDbIterator, StructuredSchema, combined_resolver, decode_row,
    load_structured_schema_from_cobble_schema,
};
use cobble::{
    GlobalSnapshotManifest, GlobalSnapshotSummary, ReadOnlyDb, ReadOptions, Reader, ReaderConfig,
    Result, ScanOptions, VolumeDescriptor,
};
use std::ops::Range;
use std::sync::Arc;

pub struct StructuredReader {
    reader: Reader,
    structured_schema: Arc<StructuredSchema>,
}

impl StructuredReader {
    pub fn open(read_config: ReaderConfig, global_snapshot_id: u64) -> Result<Self> {
        let volumes = read_config.volumes.clone();
        let resolver = combined_resolver(None);
        let reader = Reader::open_with_resolver(read_config, global_snapshot_id, Some(resolver))?;
        let structured_schema = load_schema_from_reader(&reader, &volumes)?;
        Ok(Self {
            reader,
            structured_schema: Arc::new(structured_schema),
        })
    }

    pub fn open_current(read_config: ReaderConfig) -> Result<Self> {
        let volumes = read_config.volumes.clone();
        let resolver = combined_resolver(None);
        let reader = Reader::open_current_with_resolver(read_config, Some(resolver))?;
        let structured_schema = load_schema_from_reader(&reader, &volumes)?;
        Ok(Self {
            reader,
            structured_schema: Arc::new(structured_schema),
        })
    }

    pub fn current_schema(&self) -> StructuredSchema {
        self.structured_schema.as_ref().clone()
    }

    // ── Read operations ─────────────────────────────────────────────────

    pub fn get(
        &mut self,
        bucket_id: u16,
        key: &[u8],
    ) -> Result<Option<Vec<Option<StructuredColumnValue>>>> {
        let raw = self.reader.get(bucket_id, key)?;
        let default_schema = self.structured_schema.projected(0, None);
        raw.map(|columns| decode_row(&default_schema, 0, columns))
            .transpose()
    }

    pub fn get_with_options(
        &mut self,
        bucket_id: u16,
        key: &[u8],
        options: &ReadOptions,
    ) -> Result<Option<Vec<Option<StructuredColumnValue>>>> {
        let raw = self.reader.get_with_options(bucket_id, key, options)?;
        let projected_schema = self.structured_schema.project_structured_family(
            options.column_family.as_deref(),
            options.column_indices.as_deref(),
        )?;
        raw.map(|columns| decode_row(&projected_schema, 0, columns))
            .transpose()
    }

    pub fn scan(
        &mut self,
        bucket_id: u16,
        range: Range<&[u8]>,
    ) -> Result<StructuredDbIterator<'static>> {
        self.scan_with_options(bucket_id, range, &ScanOptions::default())
    }

    pub fn scan_with_options(
        &mut self,
        bucket_id: u16,
        range: Range<&[u8]>,
        options: &ScanOptions,
    ) -> Result<StructuredDbIterator<'static>> {
        let inner = self.reader.scan_with_options(bucket_id, range, options)?;
        let projected_schema = self.structured_schema.project_structured_family(
            options.column_family.as_deref(),
            options.column_indices.as_deref(),
        )?;
        Ok(StructuredDbIterator::new(inner, projected_schema, 0))
    }

    // ── Snapshot management ─────────────────────────────────────────────

    pub fn refresh(&mut self) -> Result<()> {
        self.reader.refresh()
    }

    pub fn read_mode(&self) -> &'static str {
        self.reader.read_mode()
    }

    pub fn configured_snapshot_id(&self) -> Option<u64> {
        self.reader.configured_snapshot_id()
    }

    pub fn current_global_snapshot(&self) -> &GlobalSnapshotManifest {
        self.reader.current_global_snapshot()
    }

    pub fn list_global_snapshots(&self) -> Result<Vec<GlobalSnapshotSummary>> {
        self.reader.list_global_snapshots()
    }

    pub fn list_global_snapshot_manifests(&self) -> Result<Vec<GlobalSnapshotManifest>> {
        self.reader.list_global_snapshot_manifests()
    }
}

/// Load the structured schema from the first shard of the reader's current global snapshot.
fn load_schema_from_reader(
    reader: &Reader,
    volumes: &[VolumeDescriptor],
) -> Result<StructuredSchema> {
    let manifest = reader.current_global_snapshot();
    let shard = manifest.shard_snapshots.first().ok_or_else(|| {
        cobble::Error::ConfigError("global snapshot has no shard snapshots".to_string())
    })?;
    let config = cobble::Config {
        volumes: volumes.to_vec(),
        total_buckets: manifest.total_buckets,
        ..cobble::Config::default()
    };
    let read_only = ReadOnlyDb::open_with_db_id_and_resolver(
        config,
        shard.snapshot_id,
        shard.db_id.clone(),
        combined_resolver(None),
    )?;
    load_structured_schema_from_cobble_schema(&read_only.current_schema())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::StructuredColumnType;
    use crate::list::{ListConfig, ListRetainMode};
    use crate::structured_single_db::StructuredSingleDb;
    use bytes::Bytes;
    use cobble::{ReadOptions, VolumeDescriptor, WriteOptions};
    use std::collections::BTreeMap;
    use std::thread;
    use std::time::Duration;
    use uuid::Uuid;

    fn test_schema() -> StructuredSchema {
        StructuredSchema {
            column_families: BTreeMap::from([(
                0,
                crate::StructuredColumnFamilySchema {
                    columns: BTreeMap::from([(
                        1,
                        StructuredColumnType::List(ListConfig {
                            max_elements: Some(3),
                            retain_mode: ListRetainMode::Last,
                            preserve_element_ttl: false,
                        }),
                    )]),
                },
            )]),
            ..Default::default()
        }
    }

    fn test_config(root: &str) -> cobble::Config {
        cobble::Config {
            volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
            num_columns: 1,
            total_buckets: 2,
            snapshot_on_flush: true,
            ..cobble::Config::default()
        }
    }

    #[test]
    fn test_structured_reader_get_scan() {
        let root = format!("/tmp/ds_reader_get_scan_{}", Uuid::new_v4());

        // Write data via StructuredSingleDb and create a global snapshot
        let mut db = StructuredSingleDb::open(test_config(&root)).unwrap();
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
        db.put(0, b"k1", 0, Bytes::from_static(b"v0")).unwrap();
        db.merge(0, b"k1", 1, vec![Bytes::from_static(b"a")])
            .unwrap();
        db.merge(0, b"k1", 1, vec![Bytes::from_static(b"b")])
            .unwrap();
        let snap_id = db.snapshot().unwrap();
        thread::sleep(Duration::from_millis(200));
        db.close().unwrap();

        // Open as StructuredReader
        let read_config = ReaderConfig {
            volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
            total_buckets: 2,
            ..ReaderConfig::default()
        };
        let mut reader = StructuredReader::open(read_config, snap_id).unwrap();

        // Verify schema was auto-loaded
        assert_eq!(reader.current_schema(), test_schema());

        // get
        let row = reader.get(0, b"k1").unwrap().expect("row exists");
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

        // scan
        let mut iter = reader.scan(0, b"k0".as_ref()..b"k9".as_ref()).unwrap();
        let first = iter.next().expect("one row").unwrap();
        assert_eq!(first.0.as_ref(), b"k1");
        assert!(iter.next().is_none());

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn test_structured_reader_open_current() {
        let root = format!("/tmp/ds_reader_current_{}", Uuid::new_v4());

        let mut db = StructuredSingleDb::open(test_config(&root)).unwrap();
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
        db.put(0, b"k1", 0, Bytes::from_static(b"v0")).unwrap();
        let _ = db.snapshot().unwrap();
        thread::sleep(Duration::from_millis(200));
        db.close().unwrap();

        let read_config = ReaderConfig {
            volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
            total_buckets: 2,
            ..ReaderConfig::default()
        };
        let mut reader = StructuredReader::open_current(read_config).unwrap();

        let row = reader.get(0, b"k1").unwrap().expect("row exists");
        assert_eq!(
            row[0],
            Some(StructuredColumnValue::Bytes(Bytes::from_static(b"v0")))
        );

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn test_structured_reader_get_with_projection_reindexes_schema() {
        let root = format!("/tmp/ds_reader_get_projection_{}", Uuid::new_v4());

        let mut db = StructuredSingleDb::open(test_config(&root)).unwrap();
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
        db.put(0, b"k1", 0, Bytes::from_static(b"v0")).unwrap();
        db.merge(0, b"k1", 1, vec![Bytes::from_static(b"a")])
            .unwrap();
        db.merge(0, b"k1", 1, vec![Bytes::from_static(b"b")])
            .unwrap();
        let snap_id = db.snapshot().unwrap();
        thread::sleep(Duration::from_millis(200));
        db.close().unwrap();

        let read_config = ReaderConfig {
            volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
            total_buckets: 2,
            ..ReaderConfig::default()
        };
        let mut reader = StructuredReader::open(read_config, snap_id).unwrap();
        let row = reader
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

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn test_structured_reader_column_family_get_scan() {
        let root = format!("/tmp/ds_reader_cf_{}", Uuid::new_v4());
        let metrics_config = ListConfig::default();

        let mut db = StructuredSingleDb::open(test_config(&root)).unwrap();
        db.update_schema()
            .add_list_column(Some("metrics".to_string()), 0, metrics_config.clone())
            .commit()
            .unwrap();
        let metrics_write = WriteOptions::with_column_family("metrics");
        db.put_with_options(0, b"k1", 0, vec![Bytes::from_static(b"a")], &metrics_write)
            .unwrap();
        db.merge_with_options(0, b"k1", 0, vec![Bytes::from_static(b"b")], &metrics_write)
            .unwrap();
        let snap_id = db.snapshot().unwrap();
        thread::sleep(Duration::from_millis(200));
        db.close().unwrap();

        let read_config = ReaderConfig {
            volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
            total_buckets: 2,
            ..ReaderConfig::default()
        };
        let mut reader = StructuredReader::open(read_config, snap_id).unwrap();
        assert!(reader.current_schema().column_families.contains_key(&1));

        let row = reader
            .get_with_options(0, b"k1", &ReadOptions::for_column_in_family("metrics", 0))
            .unwrap()
            .expect("row exists");
        assert_eq!(
            row[0],
            Some(StructuredColumnValue::List(vec![
                Bytes::from_static(b"a"),
                Bytes::from_static(b"b"),
            ]))
        );

        let mut iter = reader
            .scan_with_options(
                0,
                b"k0".as_ref()..b"k9".as_ref(),
                &ScanOptions::for_column(0).with_column_family("metrics"),
            )
            .unwrap();
        let first = iter.next().expect("one row").unwrap();
        assert_eq!(first.0.as_ref(), b"k1");
        assert_eq!(
            first.1[0],
            Some(StructuredColumnValue::List(vec![
                Bytes::from_static(b"a"),
                Bytes::from_static(b"b"),
            ]))
        );
        assert!(iter.next().is_none());

        let _ = std::fs::remove_dir_all(root);
    }
}
