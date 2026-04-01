use crate::structured_db::{
    StructuredColumnValue, StructuredDbIterator, StructuredSchema, combined_resolver, decode_row,
    load_structured_schema_from_cobble_schema, project_decoded_row_for_read,
    project_structured_schema_for_scan,
};
use cobble::{Config, MergeOperatorResolver, ReadOnlyDb, ReadOptions, Result, ScanOptions};
use std::ops::Range;
use std::sync::Arc;

pub struct StructuredReadOnlyDb {
    db: ReadOnlyDb,
    structured_schema: Arc<StructuredSchema>,
}

impl StructuredReadOnlyDb {
    pub fn open(config: Config, snapshot_id: u64, db_id: String) -> Result<Self> {
        Self::open_with_resolver(config, snapshot_id, db_id, None)
    }

    pub fn open_with_resolver(
        config: Config,
        snapshot_id: u64,
        db_id: String,
        resolver: Option<Arc<dyn MergeOperatorResolver>>,
    ) -> Result<Self> {
        let db = ReadOnlyDb::open_with_db_id_and_resolver(
            config,
            snapshot_id,
            db_id,
            combined_resolver(resolver),
        )?;
        let structured_schema = load_structured_schema_from_cobble_schema(&db.current_schema())?;
        Ok(Self {
            db,
            structured_schema: Arc::new(structured_schema),
        })
    }

    pub fn id(&self) -> &str {
        self.db.id()
    }

    pub fn structured_schema(&self) -> &StructuredSchema {
        self.structured_schema.as_ref()
    }

    pub fn get(
        &self,
        bucket: u16,
        key: &[u8],
    ) -> Result<Option<Vec<Option<StructuredColumnValue>>>> {
        let raw = self.db.get(bucket, key)?;
        raw.map(|columns| decode_row(&self.structured_schema, 0, columns))
            .transpose()
    }

    pub fn get_with_options(
        &self,
        bucket: u16,
        key: &[u8],
        options: &ReadOptions,
    ) -> Result<Option<Vec<Option<StructuredColumnValue>>>> {
        let raw = if options.column_indices.is_some() {
            self.db.get(bucket, key)?
        } else {
            self.db.get_with_options(bucket, key, options)?
        };
        raw.map(|columns| decode_row(&self.structured_schema, 0, columns))
            .transpose()
            .map(|row| row.map(|decoded| project_decoded_row_for_read(decoded, options)))
    }

    pub fn scan(&self, bucket: u16, range: Range<&[u8]>) -> Result<StructuredDbIterator<'static>> {
        self.scan_with_options(bucket, range, &ScanOptions::default())
    }

    pub fn scan_with_options(
        &self,
        bucket: u16,
        range: Range<&[u8]>,
        options: &ScanOptions,
    ) -> Result<StructuredDbIterator<'static>> {
        let inner = self.db.scan_with_options(bucket, range, options)?;
        let projected_schema = project_structured_schema_for_scan(&self.structured_schema, options);
        Ok(StructuredDbIterator::new(inner, projected_schema, 0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::StructuredColumnType;
    use crate::list::{ListConfig, ListRetainMode};
    use crate::structured_db::StructuredDb;
    use bytes::Bytes;
    use cobble::{ReadOptions, VolumeDescriptor};
    use std::collections::BTreeMap;
    use std::thread;
    use std::time::Duration;
    use uuid::Uuid;

    fn test_schema() -> StructuredSchema {
        StructuredSchema {
            columns: BTreeMap::from([(
                1,
                StructuredColumnType::List(ListConfig {
                    max_elements: Some(3),
                    retain_mode: ListRetainMode::Last,
                    preserve_element_ttl: false,
                }),
            )]),
        }
    }

    fn test_config(root: &str) -> Config {
        Config {
            volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
            num_columns: 2,
            snapshot_on_flush: true,
            ..Config::default()
        }
    }

    #[test]
    fn test_structured_read_only_db_get_scan() {
        let root = format!("/tmp/ds_readonly_get_scan_{}", Uuid::new_v4());
        let config = test_config(&root);

        // Write data using StructuredDb
        let db = StructuredDb::open(config.clone(), vec![0u16..=0u16], test_schema()).unwrap();
        db.put(0, b"k1", 0, Bytes::from_static(b"v0")).unwrap();
        db.merge(0, b"k1", 1, vec![Bytes::from_static(b"a")])
            .unwrap();
        db.merge(0, b"k1", 1, vec![Bytes::from_static(b"b")])
            .unwrap();
        let snap_id = db.snapshot().unwrap();
        thread::sleep(Duration::from_millis(200));
        let db_id = db.id().to_string();
        db.close().unwrap();

        // Open as StructuredReadOnlyDb
        let rodb = StructuredReadOnlyDb::open(config, snap_id, db_id).unwrap();

        // Verify schema was auto-loaded
        assert_eq!(rodb.structured_schema(), &test_schema());

        // get
        let row = rodb.get(0, b"k1").unwrap().expect("row exists");
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
        let mut iter = rodb.scan(0, b"k0".as_ref()..b"k9".as_ref()).unwrap();
        let first = iter.next().expect("one row").unwrap();
        assert_eq!(first.0.as_ref(), b"k1");
        assert!(iter.next().is_none());

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn test_structured_read_only_db_missing_key() {
        let root = format!("/tmp/ds_readonly_missing_{}", Uuid::new_v4());
        let config = test_config(&root);

        let db = StructuredDb::open(config.clone(), vec![0u16..=0u16], test_schema()).unwrap();
        db.put(0, b"k1", 0, Bytes::from_static(b"v0")).unwrap();
        let snap_id = db.snapshot().unwrap();
        thread::sleep(Duration::from_millis(200));
        let db_id = db.id().to_string();
        db.close().unwrap();

        let rodb = StructuredReadOnlyDb::open(config, snap_id, db_id).unwrap();
        assert!(rodb.get(0, b"no-such-key").unwrap().is_none());

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn test_structured_read_only_db_get_with_projection_reindexes_schema() {
        let root = format!("/tmp/ds_readonly_get_projection_{}", Uuid::new_v4());
        let config = test_config(&root);

        let db = StructuredDb::open(config.clone(), vec![0u16..=0u16], test_schema()).unwrap();
        db.put(0, b"k1", 0, Bytes::from_static(b"v0")).unwrap();
        db.merge(0, b"k1", 1, vec![Bytes::from_static(b"a")])
            .unwrap();
        db.merge(0, b"k1", 1, vec![Bytes::from_static(b"b")])
            .unwrap();
        let snap_id = db.snapshot().unwrap();
        thread::sleep(Duration::from_millis(200));
        let db_id = db.id().to_string();
        db.close().unwrap();

        let rodb = StructuredReadOnlyDb::open(config, snap_id, db_id).unwrap();
        let row = rodb
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
}
