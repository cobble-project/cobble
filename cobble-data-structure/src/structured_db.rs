use crate::list::{
    LIST_OPERATOR_ID, ListConfig, decode_list_for_read, encode_list_for_write, list_operator,
    list_operator_from_metadata,
};
use arc_swap::ArcSwapOption;
use bytes::Bytes;
use cobble::{
    BytesMergeOperator, Config, Db, DbIterator, Error, MergeOperatorResolver, ReadOptions, Result,
    ScanOptions, Schema, SchemaBuilder, ShardSnapshotInput, WriteBatch, WriteOptions,
};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;
use std::ops::{Range, RangeInclusive};
use std::sync::Arc;

const DEFAULT_COLUMN_FAMILY_ID: u8 = 0;
const DEFAULT_COLUMN_FAMILY_NAME: &str = "default";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StructuredSchema {
    pub column_family_ids: BTreeMap<String, u8>,
    pub column_families: BTreeMap<u8, StructuredColumnFamilySchema>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StructuredColumnFamilySchema {
    pub columns: BTreeMap<u16, StructuredColumnType>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum StructuredColumnType {
    #[default]
    Bytes,
    List(ListConfig),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StructuredColumnValue {
    Bytes(Bytes),
    List(Vec<Bytes>),
}

impl StructuredColumnFamilySchema {
    fn structured_column_type(&self, column: u16) -> &StructuredColumnType {
        self.columns
            .get(&column)
            .unwrap_or(&StructuredColumnType::Bytes)
    }

    fn insert_column(&mut self, column: u16, column_type: StructuredColumnType) {
        let mut shifted = BTreeMap::new();
        for (existing_column, existing_type) in std::mem::take(&mut self.columns) {
            let shifted_column = if existing_column >= column {
                existing_column + 1
            } else {
                existing_column
            };
            shifted.insert(shifted_column, existing_type);
        }
        if !matches!(column_type, StructuredColumnType::Bytes) {
            shifted.insert(column, column_type);
        }
        self.columns = shifted;
    }

    fn delete_column(&mut self, column: u16) {
        let mut shifted = BTreeMap::new();
        for (existing_column, existing_type) in std::mem::take(&mut self.columns) {
            if existing_column == column {
                continue;
            }
            let shifted_column = if existing_column > column {
                existing_column - 1
            } else {
                existing_column
            };
            shifted.insert(shifted_column, existing_type);
        }
        self.columns = shifted;
    }
}

impl StructuredSchema {
    /// Returns structured column families keyed by public column-family name.
    ///
    /// The returned view always includes the default family, even when that family
    /// only relies on implicit `Bytes` columns and therefore has no explicit
    /// structured-column entries recorded internally.
    pub fn column_families(&self) -> BTreeMap<String, StructuredColumnFamilySchema> {
        let mut families = BTreeMap::new();
        families.insert(
            DEFAULT_COLUMN_FAMILY_NAME.to_string(),
            self.column_families
                .get(&DEFAULT_COLUMN_FAMILY_ID)
                .cloned()
                .unwrap_or_default(),
        );
        for (name, &id) in &self.column_family_ids {
            if name == DEFAULT_COLUMN_FAMILY_NAME {
                continue;
            }
            families.insert(
                name.clone(),
                self.column_families.get(&id).cloned().unwrap_or_default(),
            );
        }
        families
    }

    pub(crate) fn resolve_column_family_id(&self, column_family: Option<&str>) -> Result<u8> {
        match column_family {
            None => Ok(DEFAULT_COLUMN_FAMILY_ID),
            Some(column_family) if column_family == DEFAULT_COLUMN_FAMILY_NAME => {
                Ok(DEFAULT_COLUMN_FAMILY_ID)
            }
            Some(column_family) => self
                .column_family_ids
                .get(column_family)
                .copied()
                .ok_or_else(|| {
                    Error::IoError(format!("Unknown column family '{}'", column_family))
                }),
        }
    }

    pub(crate) fn projected(
        &self,
        column_family_id: u8,
        column_indices: Option<&[usize]>,
    ) -> StructuredColumnFamilySchema {
        let columns = self
            .column_families
            .get(&column_family_id)
            .map(|family| &family.columns);
        let Some(indices) = column_indices else {
            return columns
                .cloned()
                .map(|columns| StructuredColumnFamilySchema { columns })
                .unwrap_or_default();
        };
        let mut projected = BTreeMap::new();
        for (projected_idx, original_idx) in indices.iter().enumerate() {
            if let Some(column_type) =
                columns.and_then(|columns| columns.get(&(*original_idx as u16)))
            {
                projected.insert(projected_idx as u16, column_type.clone());
            }
        }
        StructuredColumnFamilySchema { columns: projected }
    }

    fn insert_structured_column(
        &mut self,
        column_family_id: u8,
        column: u16,
        column_type: StructuredColumnType,
    ) {
        self.column_families
            .entry(column_family_id)
            .or_default()
            .insert_column(column, column_type);
    }

    fn delete_structured_column(&mut self, column_family_id: u8, column: u16) {
        self.column_families
            .entry(column_family_id)
            .or_default()
            .delete_column(column);
    }

    pub(crate) fn project_structured_family(
        &self,
        column_family: Option<&str>,
        column_indices: Option<&[usize]>,
    ) -> Result<Arc<StructuredColumnFamilySchema>> {
        let column_family_id = self.resolve_column_family_id(column_family)?;
        Ok(Arc::new(self.projected(column_family_id, column_indices)))
    }
}

impl Default for StructuredSchema {
    fn default() -> Self {
        Self {
            column_family_ids: BTreeMap::from([(
                DEFAULT_COLUMN_FAMILY_NAME.to_string(),
                DEFAULT_COLUMN_FAMILY_ID,
            )]),
            column_families: BTreeMap::new(),
        }
    }
}

pub(crate) fn encode_for_write(
    schema: &StructuredSchema,
    column_family: Option<&str>,
    now_seconds: u32,
    column: u16,
    value: StructuredColumnValue,
    ttl_seconds: Option<u32>,
) -> Result<Bytes> {
    let column_family_id = schema.resolve_column_family_id(column_family)?;
    match (
        schema
            .column_families
            .get(&column_family_id)
            .map(|family| family.structured_column_type(column))
            .unwrap_or(&StructuredColumnType::Bytes),
        value,
    ) {
        (StructuredColumnType::Bytes, StructuredColumnValue::Bytes(value)) => Ok(value),
        (StructuredColumnType::List(config), StructuredColumnValue::List(elements)) => {
            encode_list_for_write(elements, config, ttl_seconds, now_seconds)
        }
        (_, _) => Err(Error::InputError(format!(
            "column {} expects a different type of value",
            column,
        ))),
    }
}

fn ensure_list_column(
    schema: &StructuredSchema,
    column_family: Option<&str>,
    column: u16,
) -> Result<()> {
    let column_family_id = schema.resolve_column_family_id(column_family)?;
    match schema
        .column_families
        .get(&column_family_id)
        .map(|family| family.structured_column_type(column))
        .unwrap_or(&StructuredColumnType::Bytes)
    {
        StructuredColumnType::List(_) => Ok(()),
        StructuredColumnType::Bytes => Err(Error::InputError(format!(
            "column {} is not a LIST column",
            column,
        ))),
    }
}

pub(crate) fn decode_row(
    schema: &StructuredColumnFamilySchema,
    now_seconds: u32,
    columns: Vec<Option<Bytes>>,
) -> Result<Vec<Option<StructuredColumnValue>>> {
    columns
        .into_iter()
        .enumerate()
        .map(|(idx, column)| {
            let Some(raw) = column else {
                return Ok(None);
            };
            match schema.structured_column_type(idx as u16) {
                StructuredColumnType::Bytes => Ok(Some(StructuredColumnValue::Bytes(raw))),
                StructuredColumnType::List(config) => Ok(Some(StructuredColumnValue::List(
                    decode_list_for_read(&raw, config, now_seconds)?,
                ))),
            }
        })
        .collect()
}

pub(crate) fn load_structured_schema_from_cobble_schema(
    schema: &Schema,
) -> Result<StructuredSchema> {
    let mut structured_schema = StructuredSchema {
        column_family_ids: schema.column_family_ids(),
        ..StructuredSchema::default()
    };
    let column_family_ids = structured_schema.column_family_ids.clone();
    for (column_family, num_columns) in schema.column_families() {
        let column_family_id = column_family_ids
            .get(&column_family)
            .copied()
            .ok_or_else(|| {
                Error::FileFormatError(format!("missing column family id for {}", column_family))
            })?;
        let operator_ids = if column_family == DEFAULT_COLUMN_FAMILY_NAME {
            schema.all_operator_ids()
        } else {
            schema.operator_ids_in_family(&column_family)?
        };
        let mut columns = BTreeMap::new();
        for column_idx in 0..num_columns {
            let operator_id = operator_ids
                .get(column_idx)
                .map(|s| s.as_str())
                .unwrap_or("");
            if operator_id == LIST_OPERATOR_ID {
                let metadata_value = schema
                    .column_metadata_at(Some(column_family.as_str()), column_idx)?
                    .ok_or_else(|| {
                        Error::FileFormatError(format!(
                            "list column {} in column family {} missing metadata",
                            column_idx, column_family
                        ))
                    })?;
                let config = serde_json::from_value::<ListConfig>(metadata_value.clone()).map_err(
                    |err| {
                        Error::FileFormatError(format!(
                            "failed to decode list config at column {} in column family {}: {}",
                            column_idx, column_family, err
                        ))
                    },
                )?;
                columns.insert(column_idx as u16, StructuredColumnType::List(config));
            }
        }
        structured_schema
            .column_families
            .insert(column_family_id, StructuredColumnFamilySchema { columns });
    }
    Ok(structured_schema)
}

pub(crate) fn combined_resolver(
    custom: Option<Arc<dyn MergeOperatorResolver>>,
) -> Arc<dyn MergeOperatorResolver> {
    Arc::new(move |id: &str, metadata: Option<&JsonValue>| {
        if let Some(operator) = list_operator_from_metadata(id, metadata) {
            return Some(operator);
        }
        custom
            .as_ref()
            .and_then(|resolver| resolver.resolve(id, metadata))
    })
}

/// Returns a `MergeOperatorResolver` that can resolve all structured data type
/// merge operators (e.g. list) from their metadata.
pub fn structured_merge_operator_resolver() -> Arc<dyn MergeOperatorResolver> {
    combined_resolver(None)
}

/// Returns the merge operator IDs that `structured_merge_operator_resolver` can resolve.
pub fn structured_resolvable_operator_ids() -> Vec<String> {
    vec![LIST_OPERATOR_ID.to_string()]
}

// ── StructuredWriteBatch ────────────────────────────────────────────────────

/// Structured write batch wrapper.
///
/// Each operation is encoded and written into the inner `cobble::WriteBatch` immediately, so we
/// avoid a second typed-op staging buffer and an extra conversion pass at flush time.
pub struct StructuredWriteBatch {
    structured_schema: Arc<StructuredSchema>,
    now_seconds: u32,
    inner: WriteBatch,
}

impl StructuredWriteBatch {
    pub(crate) fn new(structured_schema: Arc<StructuredSchema>, now_seconds: u32) -> Self {
        Self {
            structured_schema,
            now_seconds,
            inner: WriteBatch::new(),
        }
    }

    pub fn put<K, V>(&mut self, bucket: u16, key: K, column: u16, value: V) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: Into<StructuredColumnValue>,
    {
        self.put_with_options(
            bucket,
            key,
            column,
            value,
            &StructuredWriteOptions::default(),
        )
    }

    pub fn put_with_options<K, V>(
        &mut self,
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
            self.now_seconds,
            column,
            value.into(),
            options.ttl_seconds(),
        )?;
        self.inner
            .put_with_options(bucket, key, column, encoded, options.as_cobble());
        Ok(())
    }

    pub fn delete<K>(&mut self, bucket: u16, key: K, column: u16)
    where
        K: AsRef<[u8]>,
    {
        self.delete_with_options(bucket, key, column, &StructuredWriteOptions::default());
    }

    pub fn delete_with_options<K>(
        &mut self,
        bucket: u16,
        key: K,
        column: u16,
        options: &StructuredWriteOptions,
    ) where
        K: AsRef<[u8]>,
    {
        self.inner
            .delete_with_options(bucket, key, column, options.as_cobble());
    }

    pub fn merge<K, V>(&mut self, bucket: u16, key: K, column: u16, value: V) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: Into<StructuredColumnValue>,
    {
        self.merge_with_options(
            bucket,
            key,
            column,
            value,
            &StructuredWriteOptions::default(),
        )
    }

    pub fn merge_with_options<K, V>(
        &mut self,
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
            self.now_seconds,
            column,
            value.into(),
            options.ttl_seconds(),
        )?;
        self.inner
            .merge_with_options(bucket, key, column, encoded, options.as_cobble());
        Ok(())
    }

    pub(crate) fn into_inner(self) -> WriteBatch {
        self.inner
    }
}

// ── StructuredColumnValue conversions ───────────────────────────────────────

impl From<Bytes> for StructuredColumnValue {
    fn from(value: Bytes) -> Self {
        Self::Bytes(value)
    }
}

impl From<Vec<u8>> for StructuredColumnValue {
    fn from(value: Vec<u8>) -> Self {
        Self::Bytes(Bytes::from(value))
    }
}

impl From<Vec<Bytes>> for StructuredColumnValue {
    fn from(value: Vec<Bytes>) -> Self {
        Self::List(value)
    }
}

impl From<Vec<Vec<u8>>> for StructuredColumnValue {
    fn from(value: Vec<Vec<u8>>) -> Self {
        Self::List(value.into_iter().map(Bytes::from).collect())
    }
}

// ── StructuredDb (formerly DataStructureDb) ─────────────────────────────────

pub struct StructuredDb {
    db: Db,
    structured_schema: Arc<StructuredSchema>,
    default_write_options: StructuredWriteOptions,
    default_read_options: StructuredReadOptions,
    default_scan_options: StructuredScanOptions,
}

#[derive(Clone, Debug)]
struct StructuredProjectionCacheEntry {
    schema_ptr: usize,
    projected_schema: Arc<StructuredColumnFamilySchema>,
}

fn resolve_structured_projection_cached(
    cache: &Arc<ArcSwapOption<StructuredProjectionCacheEntry>>,
    structured_schema: &Arc<StructuredSchema>,
    column_family: Option<&str>,
    column_indices: Option<&[usize]>,
) -> Result<Arc<StructuredColumnFamilySchema>> {
    let schema_ptr = Arc::as_ptr(structured_schema) as usize;
    if let Some(entry) = cache.load_full()
        && entry.schema_ptr == schema_ptr
    {
        return Ok(Arc::clone(&entry.projected_schema));
    }
    let projected = structured_schema.project_structured_family(column_family, column_indices)?;
    cache.store(Some(Arc::new(StructuredProjectionCacheEntry {
        schema_ptr,
        projected_schema: Arc::clone(&projected),
    })));
    Ok(projected)
}

#[derive(Clone, Debug, Default)]
pub struct StructuredWriteOptions {
    inner: WriteOptions,
}

impl StructuredWriteOptions {
    pub fn with_ttl(ttl_seconds: u32) -> Self {
        Self {
            inner: WriteOptions::with_ttl(ttl_seconds),
        }
    }

    pub fn with_column_family(column_family: impl Into<String>) -> Self {
        Self {
            inner: WriteOptions::with_column_family(column_family),
        }
    }

    pub fn as_cobble(&self) -> &WriteOptions {
        &self.inner
    }

    pub fn into_cobble(self) -> WriteOptions {
        self.inner
    }

    pub fn ttl_seconds(&self) -> Option<u32> {
        self.inner.ttl_seconds
    }

    pub fn column_family(&self) -> Option<&str> {
        self.inner.column_family.as_deref()
    }
}

impl From<WriteOptions> for StructuredWriteOptions {
    fn from(value: WriteOptions) -> Self {
        Self { inner: value }
    }
}

impl From<StructuredWriteOptions> for WriteOptions {
    fn from(value: StructuredWriteOptions) -> Self {
        value.inner
    }
}

#[derive(Clone, Debug)]
pub struct StructuredReadOptions {
    inner: ReadOptions,
    projected_schema_cache: Arc<ArcSwapOption<StructuredProjectionCacheEntry>>,
}

impl Default for StructuredReadOptions {
    fn default() -> Self {
        Self::from(ReadOptions::default())
    }
}

impl StructuredReadOptions {
    pub fn for_column(column_index: usize) -> Self {
        Self::from(ReadOptions::for_column(column_index))
    }

    pub fn for_columns(column_indices: Vec<usize>) -> Self {
        Self::from(ReadOptions::for_columns(column_indices))
    }

    pub fn for_column_in_family(column_family: impl Into<String>, column_index: usize) -> Self {
        Self::from(ReadOptions::for_column_in_family(
            column_family,
            column_index,
        ))
    }

    pub fn for_columns_in_family(
        column_family: impl Into<String>,
        column_indices: Vec<usize>,
    ) -> Self {
        Self::from(ReadOptions::for_columns_in_family(
            column_family,
            column_indices,
        ))
    }

    pub fn with_column_family(mut self, column_family: impl Into<String>) -> Self {
        self.inner = self.inner.with_column_family(column_family);
        self.projected_schema_cache = Arc::new(ArcSwapOption::empty());
        self
    }

    pub fn as_cobble(&self) -> &ReadOptions {
        &self.inner
    }

    pub fn into_cobble(self) -> ReadOptions {
        self.inner
    }

    pub(crate) fn resolve_projected_schema_cached(
        &self,
        structured_schema: &Arc<StructuredSchema>,
    ) -> Result<Arc<StructuredColumnFamilySchema>> {
        resolve_structured_projection_cached(
            &self.projected_schema_cache,
            structured_schema,
            self.inner.column_family.as_deref(),
            self.inner.column_indices.as_deref(),
        )
    }
}

impl From<ReadOptions> for StructuredReadOptions {
    fn from(value: ReadOptions) -> Self {
        Self {
            inner: value,
            projected_schema_cache: Arc::new(ArcSwapOption::empty()),
        }
    }
}

impl From<StructuredReadOptions> for ReadOptions {
    fn from(value: StructuredReadOptions) -> Self {
        value.inner
    }
}

#[derive(Clone, Debug)]
pub struct StructuredScanOptions {
    inner: ScanOptions,
    projected_schema_cache: Arc<ArcSwapOption<StructuredProjectionCacheEntry>>,
}

impl Default for StructuredScanOptions {
    fn default() -> Self {
        Self::from(ScanOptions::default())
    }
}

impl StructuredScanOptions {
    pub fn for_column(column_index: usize) -> Self {
        Self::from(ScanOptions::for_column(column_index))
    }

    pub fn for_columns(column_indices: Vec<usize>) -> Self {
        Self::from(ScanOptions::for_columns(column_indices))
    }

    pub fn with_column_family(mut self, column_family: impl Into<String>) -> Self {
        self.inner = self.inner.with_column_family(column_family);
        self.projected_schema_cache = Arc::new(ArcSwapOption::empty());
        self
    }

    pub fn as_cobble(&self) -> &ScanOptions {
        &self.inner
    }

    pub fn into_cobble(self) -> ScanOptions {
        self.inner
    }

    pub(crate) fn resolve_projected_schema_cached(
        &self,
        structured_schema: &Arc<StructuredSchema>,
    ) -> Result<Arc<StructuredColumnFamilySchema>> {
        resolve_structured_projection_cached(
            &self.projected_schema_cache,
            structured_schema,
            self.inner.column_family.as_deref(),
            self.inner.column_indices.as_deref(),
        )
    }
}

impl From<ScanOptions> for StructuredScanOptions {
    fn from(value: ScanOptions) -> Self {
        Self {
            inner: value,
            projected_schema_cache: Arc::new(ArcSwapOption::empty()),
        }
    }
}

impl From<StructuredScanOptions> for ScanOptions {
    fn from(value: StructuredScanOptions) -> Self {
        value.inner
    }
}

pub trait StructuredSchemaOwner {
    fn current_structured_schema(&self) -> StructuredSchema;
    fn begin_core_schema_update(&self) -> SchemaBuilder;
    fn reload_structured_schema_from_core(&mut self) -> Result<StructuredSchema>;
}

pub struct StructuredSchemaBuilder<'a, O: StructuredSchemaOwner> {
    owner: &'a mut O,
    schema: StructuredSchema,
    inner: Option<SchemaBuilder>,
    pending_error: Option<Error>,
}

impl<'a, O: StructuredSchemaOwner> StructuredSchemaBuilder<'a, O> {
    pub fn new(owner: &'a mut O) -> Self {
        let schema = owner.current_structured_schema();
        let inner = owner.begin_core_schema_update();
        Self {
            owner,
            schema,
            inner: Some(inner),
            pending_error: None,
        }
    }

    pub fn add_bytes_column(&mut self, column_family: Option<String>, column: u16) -> &mut Self {
        let Some((column_family_name, core_column_family)) =
            self.normalize_column_family_or_record_error(column_family)
        else {
            return self;
        };
        let Some(column_family_id) = self.apply_inner(|inner| {
            let column_family_id = match core_column_family.as_ref() {
                Some(column_family) => inner.ensure_column_family_exists(column_family.clone())?,
                None => DEFAULT_COLUMN_FAMILY_ID,
            };
            inner.add_column(
                column as usize,
                Some(Arc::new(BytesMergeOperator)),
                None,
                core_column_family.clone(),
            )?;
            Ok(column_family_id)
        }) else {
            return self;
        };
        self.sync_structured_column_family_id(&column_family_name, column_family_id);
        if let Some(column_family_id) =
            self.structured_column_family_id_or_record_error(&column_family_name)
        {
            self.schema.insert_structured_column(
                column_family_id,
                column,
                StructuredColumnType::Bytes,
            );
        }
        self
    }

    pub fn add_list_column(
        &mut self,
        column_family: Option<String>,
        column: u16,
        config: ListConfig,
    ) -> &mut Self {
        let Some((column_family_name, core_column_family)) =
            self.normalize_column_family_or_record_error(column_family)
        else {
            return self;
        };
        let Some(column_family_id) = self.apply_inner(|inner| {
            let column_family_id = match core_column_family.as_ref() {
                Some(column_family) => inner.ensure_column_family_exists(column_family.clone())?,
                None => DEFAULT_COLUMN_FAMILY_ID,
            };
            inner.add_column(
                column as usize,
                Some(list_operator(config.clone())),
                None,
                core_column_family.clone(),
            )?;
            Ok(column_family_id)
        }) else {
            return self;
        };
        self.sync_structured_column_family_id(&column_family_name, column_family_id);
        if let Some(column_family_id) =
            self.structured_column_family_id_or_record_error(&column_family_name)
        {
            self.schema.insert_structured_column(
                column_family_id,
                column,
                StructuredColumnType::List(config),
            );
        }
        self
    }

    pub fn delete_column(&mut self, column_family: Option<String>, column: u16) -> &mut Self {
        let Some((column_family_name, core_column_family)) =
            self.normalize_column_family_or_record_error(column_family)
        else {
            return self;
        };
        let Some(()) = self
            .apply_inner(|inner| inner.delete_column(core_column_family.clone(), column as usize))
        else {
            return self;
        };
        if let Some(column_family_id) =
            self.structured_column_family_id_or_record_error(&column_family_name)
        {
            self.schema
                .delete_structured_column(column_family_id, column);
        }
        self
    }

    pub fn set_column_family_options(
        &mut self,
        column_family: Option<String>,
        options: cobble::ColumnFamilyOptions,
    ) -> &mut Self {
        let Some((column_family_name, core_column_family)) =
            self.normalize_column_family_or_record_error(column_family)
        else {
            return self;
        };
        let Some(column_family_id) = self.apply_inner(|inner| {
            let column_family_id = match core_column_family.as_ref() {
                Some(column_family) => inner.ensure_column_family_exists(column_family.clone())?,
                None => DEFAULT_COLUMN_FAMILY_ID,
            };
            inner.set_column_family_options(core_column_family.clone(), options.clone())?;
            Ok(column_family_id)
        }) else {
            return self;
        };
        self.sync_structured_column_family_id(&column_family_name, column_family_id);
        self
    }

    pub fn current_schema(&self) -> &StructuredSchema {
        &self.schema
    }

    fn ensure_structured_column_family_id(&self, column_family: &str) -> Result<u8> {
        if column_family == DEFAULT_COLUMN_FAMILY_NAME {
            return Ok(DEFAULT_COLUMN_FAMILY_ID);
        }
        self.schema
            .column_family_ids
            .get(column_family)
            .copied()
            .ok_or_else(|| {
                Error::InvalidState(format!(
                    "structured schema missing column family id for {}",
                    column_family
                ))
            })
    }

    fn sync_structured_column_family_id(&mut self, column_family: &str, column_family_id: u8) {
        if column_family == DEFAULT_COLUMN_FAMILY_NAME {
            return;
        }
        self.schema
            .column_family_ids
            .insert(column_family.to_string(), column_family_id);
    }

    fn normalize_column_family_or_record_error(
        &mut self,
        column_family: Option<String>,
    ) -> Option<(String, Option<String>)> {
        match normalize_structured_column_family_name(column_family) {
            Ok(names) => Some(names),
            Err(err) => {
                self.pending_error = Some(err);
                None
            }
        }
    }

    fn structured_column_family_id_or_record_error(&mut self, column_family: &str) -> Option<u8> {
        match self.ensure_structured_column_family_id(column_family) {
            Ok(column_family_id) => Some(column_family_id),
            Err(err) => {
                self.pending_error = Some(err);
                None
            }
        }
    }

    pub fn commit(&mut self) -> Result<StructuredSchema> {
        if let Some(err) = self.pending_error.take() {
            // Drop the inner schema builder immediately so its DB access guard
            // is released even when commit fails.
            self.inner.take();
            return Err(err);
        }
        let inner = self
            .inner
            .take()
            .ok_or_else(|| Error::InvalidState("schema builder already committed".to_string()))?;
        inner.commit();
        self.owner.reload_structured_schema_from_core()
    }

    fn apply_inner<T, F>(&mut self, f: F) -> Option<T>
    where
        F: FnOnce(&mut SchemaBuilder) -> Result<T>,
    {
        if self.pending_error.is_some() {
            return None;
        }
        let Some(inner) = self.inner.as_mut() else {
            self.pending_error = Some(Error::InvalidState(
                "schema builder already committed".to_string(),
            ));
            return None;
        };
        match f(inner) {
            Ok(value) => Some(value),
            Err(err) => {
                self.pending_error = Some(err);
                None
            }
        }
    }
}

fn normalize_structured_column_family_name(
    column_family: Option<String>,
) -> Result<(String, Option<String>)> {
    let normalized = match column_family {
        Some(column_family) => {
            let normalized = column_family.trim().to_string();
            if normalized.is_empty() {
                return Err(Error::InvalidState(
                    "column family name cannot be empty".to_string(),
                ));
            }
            normalized
        }
        None => DEFAULT_COLUMN_FAMILY_NAME.to_string(),
    };
    let core_column_family = if normalized == DEFAULT_COLUMN_FAMILY_NAME {
        None
    } else {
        Some(normalized.clone())
    };
    Ok((normalized, core_column_family))
}

impl StructuredDb {
    fn from_db(db: Db) -> Result<Self> {
        let structured_schema = Arc::new(load_structured_schema_from_cobble_schema(
            &db.current_schema(),
        )?);
        Ok(Self {
            db,
            structured_schema,
            default_write_options: StructuredWriteOptions::default(),
            default_read_options: StructuredReadOptions::default(),
            default_scan_options: StructuredScanOptions::default(),
        })
    }

    fn reset_default_options(&mut self) {
        self.default_write_options = StructuredWriteOptions::default();
        self.default_read_options = StructuredReadOptions::default();
        self.default_scan_options = StructuredScanOptions::default();
    }

    pub fn open(config: Config, bucket_ranges: Vec<RangeInclusive<u16>>) -> Result<Self> {
        Self::from_db(Db::open(config, bucket_ranges)?)
    }

    pub fn open_from_snapshot(
        config: Config,
        snapshot_id: u64,
        db_id: impl Into<String>,
    ) -> Result<Self> {
        Self::open_from_snapshot_with_resolver(config, snapshot_id, db_id, None)
    }

    pub fn open_from_snapshot_with_resolver(
        config: Config,
        snapshot_id: u64,
        db_id: impl Into<String>,
        resolver: Option<Arc<dyn MergeOperatorResolver>>,
    ) -> Result<Self> {
        Self::from_db(Db::open_from_snapshot_with_resolver(
            config,
            snapshot_id,
            db_id,
            Some(combined_resolver(resolver)),
        )?)
    }

    pub fn open_new_with_snapshot(
        config: Config,
        snapshot_id: u64,
        db_id: impl Into<String>,
    ) -> Result<Self> {
        Self::open_new_with_snapshot_with_resolver(config, snapshot_id, db_id, None)
    }

    pub fn open_new_with_snapshot_with_resolver(
        config: Config,
        snapshot_id: u64,
        db_id: impl Into<String>,
        resolver: Option<Arc<dyn MergeOperatorResolver>>,
    ) -> Result<Self> {
        let db_id = db_id.into();
        Self::from_db(Db::open_new_with_snapshot_with_resolver(
            config,
            snapshot_id,
            &db_id,
            Some(combined_resolver(resolver)),
        )?)
    }

    pub fn open_new_with_manifest_path(
        config: Config,
        manifest_path: impl Into<String>,
    ) -> Result<Self> {
        Self::open_new_with_manifest_path_with_resolver(config, manifest_path, None)
    }

    pub fn open_new_with_manifest_path_with_resolver(
        config: Config,
        manifest_path: impl Into<String>,
        resolver: Option<Arc<dyn MergeOperatorResolver>>,
    ) -> Result<Self> {
        Self::from_db(Db::open_new_with_manifest_path_with_resolver(
            config,
            manifest_path,
            Some(combined_resolver(resolver)),
        )?)
    }

    pub fn resume(config: Config, db_id: impl Into<String>) -> Result<Self> {
        Self::resume_with_resolver(config, db_id, None)
    }

    pub fn resume_with_resolver(
        config: Config,
        db_id: impl Into<String>,
        resolver: Option<Arc<dyn MergeOperatorResolver>>,
    ) -> Result<Self> {
        Self::from_db(Db::resume_with_resolver(
            config,
            db_id,
            Some(combined_resolver(resolver)),
        )?)
    }

    pub fn expand_bucket(
        &self,
        source_db_id: impl Into<String>,
        snapshot_id: Option<u64>,
        ranges: Option<Vec<RangeInclusive<u16>>>,
    ) -> Result<u64> {
        self.db.expand_bucket(source_db_id, snapshot_id, ranges)
    }

    pub fn shrink_bucket(&self, ranges: Vec<RangeInclusive<u16>>) -> Result<u64> {
        self.db.shrink_bucket(ranges)
    }

    pub fn id(&self) -> &str {
        self.db.id()
    }

    pub fn current_schema(&self) -> StructuredSchema {
        self.structured_schema.as_ref().clone()
    }

    pub fn update_schema(&mut self) -> StructuredSchemaBuilder<'_, Self> {
        StructuredSchemaBuilder::new(self)
    }

    pub fn reload_schema(&mut self) -> Result<()> {
        let schema = load_structured_schema_from_cobble_schema(&self.db.current_schema())?;
        self.structured_schema = Arc::new(schema);
        self.reset_default_options();
        Ok(())
    }

    pub fn apply_schema(
        &mut self,
        structured_schema: StructuredSchema,
    ) -> Result<StructuredSchema> {
        persist_structured_schema_on_db(&self.db, &structured_schema)?;
        let reloaded = load_structured_schema_from_cobble_schema(&self.db.current_schema())?;
        self.structured_schema = Arc::new(reloaded.clone());
        self.reset_default_options();
        Ok(reloaded)
    }

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
            self.db.now_seconds(),
            column,
            value.into(),
            options.ttl_seconds(),
        )?;
        self.db
            .put_with_options(bucket, key, column, encoded, options.as_cobble())
    }

    pub fn put_encoded_list<K, B>(&self, bucket: u16, key: K, column: u16, encoded: B) -> Result<()>
    where
        K: AsRef<[u8]>,
        B: Into<Bytes>,
    {
        self.put_encoded_list_with_options(
            bucket,
            key,
            column,
            encoded,
            &self.default_write_options,
        )
    }

    pub fn put_encoded_list_with_options<K, B>(
        &self,
        bucket: u16,
        key: K,
        column: u16,
        encoded: B,
        options: &StructuredWriteOptions,
    ) -> Result<()>
    where
        K: AsRef<[u8]>,
        B: Into<Bytes>,
    {
        ensure_list_column(&self.structured_schema, options.column_family(), column)?;
        self.db
            .put_with_options(bucket, key, column, encoded.into(), options.as_cobble())
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
            self.db.now_seconds(),
            column,
            value.into(),
            options.ttl_seconds(),
        )?;
        self.db
            .merge_with_options(bucket, key, column, encoded, options.as_cobble())
    }

    pub fn merge_encoded_list<K, B>(
        &self,
        bucket: u16,
        key: K,
        column: u16,
        encoded: B,
    ) -> Result<()>
    where
        K: AsRef<[u8]>,
        B: Into<Bytes>,
    {
        self.merge_encoded_list_with_options(
            bucket,
            key,
            column,
            encoded,
            &self.default_write_options,
        )
    }

    pub fn merge_encoded_list_with_options<K, B>(
        &self,
        bucket: u16,
        key: K,
        column: u16,
        encoded: B,
        options: &StructuredWriteOptions,
    ) -> Result<()>
    where
        K: AsRef<[u8]>,
        B: Into<Bytes>,
    {
        ensure_list_column(&self.structured_schema, options.column_family(), column)?;
        self.db
            .merge_with_options(bucket, key, column, encoded.into(), options.as_cobble())
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
        StructuredWriteBatch::new(Arc::clone(&self.structured_schema), self.db.now_seconds())
    }

    pub fn write_batch(&self, batch: StructuredWriteBatch) -> Result<()> {
        self.db.write_batch(batch.into_inner())
    }

    pub fn get<K>(&self, bucket: u16, key: K) -> Result<Option<Vec<Option<StructuredColumnValue>>>>
    where
        K: AsRef<[u8]>,
    {
        self.get_with_options(bucket, key, &self.default_read_options)
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

    pub fn snapshot(&self) -> Result<u64> {
        self.db.snapshot()
    }

    pub fn snapshot_with_callback<F>(&self, callback: F) -> Result<u64>
    where
        F: Fn(Result<ShardSnapshotInput>) + Send + Sync + 'static,
    {
        self.db.snapshot_with_callback(callback)
    }

    pub fn expire_snapshot(&self, snapshot_id: u64) -> Result<bool> {
        self.db.expire_snapshot(snapshot_id)
    }

    pub fn retain_snapshot(&self, snapshot_id: u64) -> bool {
        self.db.retain_snapshot(snapshot_id)
    }

    pub fn shard_snapshot_input(&self, snapshot_id: u64) -> Result<ShardSnapshotInput> {
        self.db.shard_snapshot_input(snapshot_id)
    }

    pub fn set_time(&self, next: u32) {
        self.db.set_time(next);
    }

    pub fn now_seconds(&self) -> u32 {
        self.db.now_seconds()
    }

    pub fn get_raw_with_options(
        &self,
        bucket: u16,
        key: &[u8],
        options: &StructuredReadOptions,
    ) -> Result<Option<Vec<Option<Bytes>>>> {
        self.db.get_with_options(bucket, key, options.as_cobble())
    }

    pub fn scan_raw<'a>(
        &'a self,
        bucket: u16,
        range: Range<&[u8]>,
        options: &StructuredScanOptions,
    ) -> Result<DbIterator<'a>> {
        self.db
            .scan_with_options(bucket, range, options.as_cobble())
    }

    pub fn close(&self) -> Result<()> {
        self.db.close()
    }

    pub fn jni_direct_buffer_pool_config(&self) -> Result<(usize, usize)> {
        self.db.jni_direct_buffer_pool_config()
    }
}

impl StructuredSchemaOwner for StructuredDb {
    fn current_structured_schema(&self) -> StructuredSchema {
        self.current_schema()
    }

    fn begin_core_schema_update(&self) -> SchemaBuilder {
        self.db.update_schema()
    }

    fn reload_structured_schema_from_core(&mut self) -> Result<StructuredSchema> {
        self.reload_schema()?;
        Ok(self.current_schema())
    }
}

/// Type alias for backward compatibility.
pub type DataStructureDb = StructuredDb;

// ── StructuredDbIterator ────────────────────────────────────────────────────

pub struct StructuredDbIterator<'a> {
    inner: DbIterator<'a>,
    structured_schema: Arc<StructuredColumnFamilySchema>,
    now_seconds: u32,
}

impl<'a> StructuredDbIterator<'a> {
    pub(crate) fn new(
        inner: DbIterator<'a>,
        structured_schema: Arc<StructuredColumnFamilySchema>,
        now_seconds: u32,
    ) -> Self {
        Self {
            inner,
            structured_schema,
            now_seconds,
        }
    }
}

impl Iterator for StructuredDbIterator<'_> {
    type Item = Result<(Bytes, Vec<Option<StructuredColumnValue>>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|item| {
            let (key, columns) = item?;
            let decoded = decode_row(&self.structured_schema, self.now_seconds, columns)?;
            Ok((key, decoded))
        })
    }
}

// ── Internal helpers ────────────────────────────────────────────────────────

pub(crate) fn persist_structured_schema_on_db(
    db: &Db,
    structured_schema: &StructuredSchema,
) -> Result<()> {
    let mut schema = db.update_schema();
    apply_structured_schema(&mut schema, structured_schema)?;
    schema.commit();
    Ok(())
}

fn apply_structured_schema(
    schema: &mut SchemaBuilder,
    structured_schema: &StructuredSchema,
) -> Result<()> {
    let column_family_names_by_id = structured_schema
        .column_family_ids
        .iter()
        .map(|(name, &id)| (id, name.clone()))
        .collect::<BTreeMap<_, _>>();
    for (column_family_id, family_schema) in &structured_schema.column_families {
        let column_family = if *column_family_id == DEFAULT_COLUMN_FAMILY_ID {
            None
        } else {
            Some(
                column_family_names_by_id
                    .get(column_family_id)
                    .cloned()
                    .ok_or_else(|| {
                        Error::InvalidState(format!(
                            "unknown structured column family id {}",
                            column_family_id
                        ))
                    })?,
            )
        };
        apply_structured_family(schema, column_family, &family_schema.columns)?;
    }
    Ok(())
}

fn apply_structured_family(
    schema: &mut SchemaBuilder,
    column_family: Option<String>,
    columns: &BTreeMap<u16, StructuredColumnType>,
) -> Result<()> {
    if let Some(column_family) = column_family.as_ref()
        && !columns.is_empty()
    {
        schema.ensure_column_family_exists(column_family.clone())?;
    }
    for (column, column_type) in columns {
        match column_type {
            StructuredColumnType::Bytes => {
                schema.set_column_operator(
                    column_family.clone(),
                    *column as usize,
                    Arc::new(BytesMergeOperator),
                )?;
                schema.clear_column_metadata(column_family.clone(), *column as usize)?;
            }
            StructuredColumnType::List(config) => {
                schema.set_column_operator(
                    column_family.clone(),
                    *column as usize,
                    list_operator(config.clone()),
                )?;
                schema.set_column_metadata(
                    column_family.clone(),
                    *column as usize,
                    serde_json::to_value(config).map_err(|err| {
                        Error::FileFormatError(format!(
                            "failed to encode list config metadata: {}",
                            err
                        ))
                    })?,
                )?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::list::{ListConfig, ListRetainMode};
    use cobble::VolumeDescriptor;
    use std::thread;
    use std::time::Duration;
    use uuid::Uuid;

    fn default_family_schema(columns: BTreeMap<u16, StructuredColumnType>) -> StructuredSchema {
        StructuredSchema {
            column_families: BTreeMap::from([(
                DEFAULT_COLUMN_FAMILY_ID,
                StructuredColumnFamilySchema { columns },
            )]),
            ..Default::default()
        }
    }

    #[test]
    fn test_structured_db_resume_loads_structured_schema() {
        let root = format!("/tmp/ds_structured_resume_{}", Uuid::new_v4());
        let config = Config {
            volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
            snapshot_on_flush: true,
            num_columns: 2,
            ..Config::default()
        };
        let structured_schema = default_family_schema(BTreeMap::from([(
            1,
            StructuredColumnType::List(ListConfig {
                max_elements: Some(2),
                retain_mode: ListRetainMode::Last,
                preserve_element_ttl: true,
            }),
        )]));
        let mut db = StructuredDb::open(config.clone(), vec![0u16..=0u16]).unwrap();
        db.apply_schema(structured_schema.clone()).unwrap();
        db.merge(0, b"k", 1, vec![Bytes::from_static(b"a")])
            .unwrap();
        let _ = db.snapshot().unwrap();
        thread::sleep(Duration::from_millis(200));
        let db_id = db.id().to_string();
        db.close().unwrap();

        let resumed = StructuredDb::resume(config, db_id).unwrap();
        assert_eq!(resumed.current_schema(), structured_schema);
        resumed.close().unwrap();
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn test_structured_db_get_and_scan_return_structured_values() {
        let root = format!("/tmp/ds_structured_get_scan_{}", Uuid::new_v4());
        let config = Config {
            volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
            num_columns: 2,
            ..Config::default()
        };
        let structured_schema = default_family_schema(BTreeMap::from([(
            1,
            StructuredColumnType::List(ListConfig {
                max_elements: Some(2),
                retain_mode: ListRetainMode::Last,
                preserve_element_ttl: false,
            }),
        )]));
        let mut db = StructuredDb::open(config, vec![0u16..=0u16]).unwrap();
        db.apply_schema(structured_schema).unwrap();
        db.put(0, b"k1", 0, Bytes::from_static(b"v0")).unwrap();
        db.merge(0, b"k1", 1, vec![Bytes::from_static(b"a")])
            .unwrap();
        db.merge(0, b"k1", 1, vec![Bytes::from_static(b"b")])
            .unwrap();
        db.merge(0, b"k1", 1, vec![Bytes::from_static(b"c")])
            .unwrap();

        let row = db.get(0, b"k1").unwrap().expect("row exists");
        assert_eq!(
            row[0],
            Some(StructuredColumnValue::Bytes(Bytes::from_static(b"v0")))
        );
        assert_eq!(
            row[1],
            Some(StructuredColumnValue::List(vec![
                Bytes::from_static(b"b"),
                Bytes::from_static(b"c")
            ]))
        );

        let mut iter = db.scan(0, b"k0".as_ref()..b"k9".as_ref()).unwrap();
        let first = iter.next().expect("one row").unwrap();
        assert_eq!(first.0.as_ref(), b"k1");
        assert_eq!(first.1.len(), 2, "scan row should have 2 columns");
        assert_eq!(
            first.1[0],
            Some(StructuredColumnValue::Bytes(Bytes::from_static(b"v0")))
        );
        assert_eq!(
            first.1[1],
            Some(StructuredColumnValue::List(vec![
                Bytes::from_static(b"b"),
                Bytes::from_static(b"c")
            ]))
        );
        assert!(iter.next().is_none());
        drop(iter);

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn test_structured_write_batch_round_trip() {
        let root = format!("/tmp/ds_structured_write_batch_{}", Uuid::new_v4());
        let config = Config {
            volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
            num_columns: 2,
            ..Config::default()
        };
        let structured_schema = default_family_schema(BTreeMap::from([(
            1,
            StructuredColumnType::List(ListConfig {
                max_elements: Some(3),
                retain_mode: ListRetainMode::Last,
                preserve_element_ttl: false,
            }),
        )]));
        let mut db = StructuredDb::open(config, vec![0u16..=0u16]).unwrap();
        db.apply_schema(structured_schema).unwrap();
        let mut batch = db.new_write_batch();
        batch.put(0, b"k1", 0, Bytes::from_static(b"v0")).unwrap();
        batch
            .merge(0, b"k1", 1, vec![Bytes::from_static(b"a")])
            .unwrap();
        batch
            .merge(0, b"k1", 1, vec![Bytes::from_static(b"b")])
            .unwrap();
        batch
            .merge(0, b"k1", 1, vec![Bytes::from_static(b"c")])
            .unwrap();
        batch.put(0, b"k2", 0, Bytes::from_static(b"v2")).unwrap();
        db.write_batch(batch).unwrap();

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
                Bytes::from_static(b"c")
            ]))
        );
        let mut iter = db.scan(0, b"k0".as_ref()..b"k9".as_ref()).unwrap();
        let first = iter.next().expect("first row").unwrap();
        assert_eq!(first.0.as_ref(), b"k1");
        let second = iter.next().expect("second row").unwrap();
        assert_eq!(second.0.as_ref(), b"k2");
        assert!(iter.next().is_none());
        drop(iter);

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn test_structured_write_batch_rejects_type_mismatch() {
        let root = format!("/tmp/ds_structured_write_batch_mismatch_{}", Uuid::new_v4());
        let config = Config {
            volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
            num_columns: 2,
            ..Config::default()
        };
        let structured_schema = default_family_schema(BTreeMap::from([(
            1,
            StructuredColumnType::List(ListConfig {
                max_elements: None,
                retain_mode: ListRetainMode::Last,
                preserve_element_ttl: false,
            }),
        )]));
        let mut db = StructuredDb::open(config, vec![0u16..=0u16]).unwrap();
        db.apply_schema(structured_schema).unwrap();
        let mut batch = db.new_write_batch();
        let err = batch
            .put(0, b"k1", 1, Bytes::from_static(b"not-a-list"))
            .expect_err("type mismatch should fail");
        match err {
            Error::InputError(msg) => assert!(msg.contains("column 1 expects")),
            other => panic!("unexpected error: {other:?}"),
        }
        db.close().unwrap();
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn test_structured_scan_with_projection_reindexes_schema() {
        let root = format!("/tmp/ds_structured_scan_projection_{}", Uuid::new_v4());
        let config = Config {
            volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
            num_columns: 2,
            ..Config::default()
        };
        let structured_schema = default_family_schema(BTreeMap::from([(
            1,
            StructuredColumnType::List(ListConfig {
                max_elements: Some(8),
                retain_mode: ListRetainMode::Last,
                preserve_element_ttl: false,
            }),
        )]));
        let mut db = StructuredDb::open(config, vec![0u16..=0u16]).unwrap();
        db.apply_schema(structured_schema).unwrap();
        db.put(0, b"k1", 0, Bytes::from_static(b"v0")).unwrap();
        db.merge(0, b"k1", 1, vec![Bytes::from_static(b"a")])
            .unwrap();
        db.merge(0, b"k1", 1, vec![Bytes::from_static(b"b")])
            .unwrap();

        let mut iter = db
            .scan_with_options(
                0,
                b"k0".as_ref()..b"k9".as_ref(),
                &StructuredScanOptions::for_column(1),
            )
            .unwrap();
        let first = iter.next().expect("one row").unwrap();
        assert_eq!(first.0.as_ref(), b"k1");
        assert_eq!(first.1.len(), 1);
        assert_eq!(
            first.1[0],
            Some(StructuredColumnValue::List(vec![
                Bytes::from_static(b"a"),
                Bytes::from_static(b"b"),
            ]))
        );
        drop(iter);

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn test_structured_get_with_projection_reindexes_schema() {
        let root = format!("/tmp/ds_structured_get_projection_{}", Uuid::new_v4());
        let config = Config {
            volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
            num_columns: 2,
            ..Config::default()
        };
        let structured_schema = default_family_schema(BTreeMap::from([(
            1,
            StructuredColumnType::List(ListConfig {
                max_elements: Some(8),
                retain_mode: ListRetainMode::Last,
                preserve_element_ttl: false,
            }),
        )]));
        let mut db = StructuredDb::open(config, vec![0u16..=0u16]).unwrap();
        db.apply_schema(structured_schema).unwrap();
        db.put(0, b"k1", 0, Bytes::from_static(b"v0")).unwrap();
        db.merge(0, b"k1", 1, vec![Bytes::from_static(b"a")])
            .unwrap();
        db.merge(0, b"k1", 1, vec![Bytes::from_static(b"b")])
            .unwrap();

        let row = db
            .get_with_options(0, b"k1", &StructuredReadOptions::for_column(1))
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

    #[test]
    fn test_structured_db_column_family_get_scan_and_write_batch() {
        let root = format!("/tmp/ds_structured_cf_{}", Uuid::new_v4());
        let config = Config {
            volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
            num_columns: 2,
            ..Config::default()
        };
        let metrics_config = ListConfig {
            max_elements: Some(8),
            retain_mode: ListRetainMode::Last,
            preserve_element_ttl: false,
        };

        let mut db = StructuredDb::open(config, vec![0u16..=0u16]).unwrap();
        let schema = db
            .update_schema()
            .add_list_column(Some("metrics".to_string()), 0, metrics_config.clone())
            .commit()
            .unwrap();
        assert_eq!(
            schema
                .column_families
                .get(&1)
                .and_then(|family| family.columns.get(&0)),
            Some(&StructuredColumnType::List(metrics_config.clone()))
        );

        let metrics_write = StructuredWriteOptions::with_column_family("metrics");
        db.put_with_options(0, b"k1", 0, vec![Bytes::from_static(b"a")], &metrics_write)
            .unwrap();
        db.merge_with_options(0, b"k1", 0, vec![Bytes::from_static(b"b")], &metrics_write)
            .unwrap();

        let row = db
            .get_with_options(
                0,
                b"k1",
                &StructuredReadOptions::for_column_in_family("metrics", 0),
            )
            .unwrap()
            .expect("row exists");
        assert_eq!(
            row[0],
            Some(StructuredColumnValue::List(vec![
                Bytes::from_static(b"a"),
                Bytes::from_static(b"b"),
            ]))
        );

        let mut iter = db
            .scan_with_options(
                0,
                b"k0".as_ref()..b"k9".as_ref(),
                &StructuredScanOptions::for_column(0).with_column_family("metrics"),
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
        drop(iter);

        let mut batch = db.new_write_batch();
        batch
            .put_with_options(0, b"k2", 0, vec![Bytes::from_static(b"c")], &metrics_write)
            .unwrap();
        db.write_batch(batch).unwrap();

        let batch_row = db
            .get_with_options(
                0,
                b"k2",
                &StructuredReadOptions::for_column_in_family("metrics", 0),
            )
            .unwrap()
            .expect("batch row exists");
        assert_eq!(
            batch_row[0],
            Some(StructuredColumnValue::List(vec![Bytes::from_static(b"c")]))
        );

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn test_structured_schema_builder_reindexes_family_local_columns_on_add_and_delete() {
        let root = format!("/tmp/ds_structured_builder_indexes_{}", Uuid::new_v4());
        let config = Config {
            volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
            num_columns: 1,
            ..Config::default()
        };
        let first = ListConfig {
            max_elements: Some(4),
            retain_mode: ListRetainMode::Last,
            preserve_element_ttl: false,
        };
        let second = ListConfig {
            max_elements: Some(6),
            retain_mode: ListRetainMode::First,
            preserve_element_ttl: true,
        };

        let mut db = StructuredDb::open(config, vec![0u16..=0u16]).unwrap();
        let schema = db
            .update_schema()
            .add_list_column(None, 1, second.clone())
            .add_list_column(None, 1, first.clone())
            .commit()
            .unwrap();
        assert_eq!(db.db.current_schema().num_columns(), 3);

        let family = schema
            .column_families
            .get(&0)
            .expect("default family schema");
        assert_eq!(
            family.columns.get(&1),
            Some(&StructuredColumnType::List(first))
        );
        assert_eq!(
            family.columns.get(&2),
            Some(&StructuredColumnType::List(second.clone()))
        );

        let schema = db.update_schema().delete_column(None, 1).commit().unwrap();
        assert_eq!(db.db.current_schema().num_columns(), 2);
        let family = schema
            .column_families
            .get(&0)
            .expect("default family schema");
        assert_eq!(
            family.columns.get(&1),
            Some(&StructuredColumnType::List(second))
        );
        assert!(!family.columns.contains_key(&2));

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn test_structured_schema_builder_normalizes_column_family_names_after_inner_success() {
        let root = format!("/tmp/ds_structured_builder_family_name_{}", Uuid::new_v4());
        let config = Config {
            volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
            num_columns: 1,
            ..Config::default()
        };
        let metrics_config = ListConfig {
            max_elements: Some(5),
            retain_mode: ListRetainMode::Last,
            preserve_element_ttl: false,
        };

        let mut db = StructuredDb::open(config, vec![0u16..=0u16]).unwrap();
        let baseline = db.current_schema();
        assert_eq!(
            baseline
                .column_families()
                .keys()
                .cloned()
                .collect::<Vec<_>>(),
            vec!["default".to_string()]
        );

        let mut invalid_builder = db.update_schema();
        invalid_builder.add_list_column(Some("   ".to_string()), 0, metrics_config.clone());
        assert_eq!(invalid_builder.current_schema(), &baseline);
        let err = invalid_builder
            .commit()
            .expect_err("empty family should fail");
        assert!(matches!(err, Error::InvalidState(msg) if msg.contains("cannot be empty")));

        let schema = db
            .update_schema()
            .add_list_column(Some(" metrics ".to_string()), 0, metrics_config.clone())
            .commit()
            .unwrap();
        assert_eq!(schema.column_family_ids.get("metrics"), Some(&1));
        assert!(!schema.column_family_ids.contains_key(" metrics "));
        assert_eq!(
            schema
                .column_families
                .get(&1)
                .and_then(|family| family.columns.get(&0)),
            Some(&StructuredColumnType::List(metrics_config))
        );
        let named_families = schema.column_families();
        assert!(named_families.contains_key("default"));
        assert_eq!(
            named_families
                .get("metrics")
                .and_then(|family| family.columns.get(&0)),
            Some(&StructuredColumnType::List(ListConfig {
                max_elements: Some(5),
                retain_mode: ListRetainMode::Last,
                preserve_element_ttl: false,
            }))
        );

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(root);
    }
}
