use crate::structured_db::{
    StructuredColumnValue, StructuredDbIterator, StructuredReadOptions, StructuredScanOptions,
    StructuredSchema, combined_resolver, decode_row, load_structured_schema_from_cobble_schema,
};
use cobble::{Config, MergeOperatorResolver, ReadOnlyDb, Result};
use std::ops::Range;
use std::sync::Arc;

pub struct StructuredReadOnlyDb {
    db: ReadOnlyDb,
    structured_schema: Arc<StructuredSchema>,
    default_read_options: StructuredReadOptions,
    default_scan_options: StructuredScanOptions,
}

impl StructuredReadOnlyDb {
    pub fn open(config: Config, snapshot_id: u64, db_id: impl Into<String>) -> Result<Self> {
        Self::open_with_resolver(config, snapshot_id, db_id, None)
    }

    pub fn open_with_resolver(
        config: Config,
        snapshot_id: u64,
        db_id: impl Into<String>,
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
            default_read_options: StructuredReadOptions::default(),
            default_scan_options: StructuredScanOptions::default(),
        })
    }

    pub fn id(&self) -> &str {
        self.db.id()
    }

    pub fn current_schema(&self) -> StructuredSchema {
        self.structured_schema.as_ref().clone()
    }

    pub fn get(
        &self,
        bucket: u16,
        key: &[u8],
    ) -> Result<Option<Vec<Option<StructuredColumnValue>>>> {
        self.get_with_options(bucket, key, &self.default_read_options)
    }

    pub fn multi_get<K: AsRef<[u8]>>(
        &self,
        keys: &[(u16, K)],
    ) -> Result<Vec<Option<Vec<Option<StructuredColumnValue>>>>> {
        self.multi_get_with_options(keys, &self.default_read_options)
    }

    pub fn multi_get_with_options<K: AsRef<[u8]>>(
        &self,
        keys: &[(u16, K)],
        options: &StructuredReadOptions,
    ) -> Result<Vec<Option<Vec<Option<StructuredColumnValue>>>>> {
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

    pub fn get_with_options(
        &self,
        bucket: u16,
        key: &[u8],
        options: &StructuredReadOptions,
    ) -> Result<Option<Vec<Option<StructuredColumnValue>>>> {
        let raw = self.db.get_with_options(bucket, key, options.as_cobble())?;
        let projected_schema = options.resolve_projected_schema_cached(&self.structured_schema)?;
        raw.map(|columns| decode_row(&projected_schema, 0, columns))
            .transpose()
    }

    pub fn scan(&self, bucket: u16, range: Range<&[u8]>) -> Result<StructuredDbIterator<'static>> {
        self.scan_with_options(bucket, range, &self.default_scan_options)
    }

    pub fn scan_with_options(
        &self,
        bucket: u16,
        range: Range<&[u8]>,
        options: &StructuredScanOptions,
    ) -> Result<StructuredDbIterator<'static>> {
        let inner = self
            .db
            .scan_with_options(bucket, range, options.as_cobble())?;
        let projected_schema = options.resolve_projected_schema_cached(&self.structured_schema)?;
        Ok(StructuredDbIterator::new(inner, projected_schema, 0))
    }
}

#[cfg(test)]
#[path = "../tests/unit/structured_read_only_db.rs"]
mod tests;
