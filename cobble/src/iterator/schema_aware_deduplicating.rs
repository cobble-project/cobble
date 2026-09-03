use crate::TimeProvider;
use crate::error::{Error, Result};
use crate::file::FileManager;
use crate::iterator::KvIterator;
use crate::row_merge::{SchemaMergePlan, SchemaValue, merge_schema_values};
use crate::schema::{Schema, SchemaManager};
use crate::ttl::TTLProvider;
use crate::r#type::KvValue;
use crate::vlog::{VlogMergeCollectorHandle, VlogStore, VlogVersion};
use bytes::Bytes;
use std::sync::Arc;

/// Deduplicates a merged compaction stream while retaining each input value's
/// physical schema. It is used only when the selected task crosses a schema
/// barrier; compatible-only tasks keep the lighter streaming path.
pub(crate) struct SchemaAwareDeduplicatingIterator<I> {
    inner: I,
    merge_plan: SchemaMergePlan<'static>,
    ttl_provider: Arc<TTLProvider>,
    vlog_store: VlogStore,
    vlog_version: Arc<VlogVersion>,
    merge_collector: Option<VlogMergeCollectorHandle>,
    current_key: Option<Bytes>,
    current_value: Option<KvValue>,
}

impl<I> SchemaAwareDeduplicatingIterator<I> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        inner: I,
        target_schema: Arc<Schema>,
        schema_manager: Arc<SchemaManager>,
        column_family_id: u8,
        ttl_provider: Arc<TTLProvider>,
        file_manager: Arc<FileManager>,
        vlog_version: Arc<VlogVersion>,
        merge_collector: Option<VlogMergeCollectorHandle>,
    ) -> Self {
        Self {
            inner,
            merge_plan: SchemaMergePlan::new_shared(
                target_schema,
                schema_manager,
                column_family_id,
            ),
            ttl_provider,
            vlog_store: VlogStore::new(file_manager, 0, 0),
            vlog_version,
            merge_collector,
            current_key: None,
            current_value: None,
        }
    }

    fn source_num_columns(&self, schema_id: u64) -> Result<usize> {
        let schema = self.merge_plan.schema_manager().schema(schema_id)?;
        Ok(schema
            .num_columns_in_family(self.merge_plan.column_family_id())
            .unwrap_or_else(|| schema.num_columns()))
    }

    fn collect_and_merge<'b>(&mut self) -> Result<()>
    where
        I: KvIterator<'b>,
    {
        loop {
            if !self.inner.valid() {
                self.current_key = None;
                self.current_value = None;
                return Ok(());
            }

            let current_key = self
                .inner
                .take_key()?
                .expect("valid compaction iterator has a key");
            let mut values = Vec::new();
            loop {
                let schema_id = self.inner.current_schema_id().ok_or_else(|| {
                    Error::InvalidState(
                        "schema-aware compaction input is missing a physical schema tag"
                            .to_string(),
                    )
                })?;
                let value = self
                    .inner
                    .take_value()?
                    .expect("valid compaction iterator has a value");
                if self.ttl_provider.expired(&value.expired_at()?) {
                    if let Some(collector) = &self.merge_collector {
                        let value = value.into_decoded(self.source_num_columns(schema_id)?)?;
                        collector.borrow_mut().collect_schema_input(&value)?;
                    }
                } else {
                    let value = value.into_decoded(self.source_num_columns(schema_id)?)?;
                    if let Some(collector) = &self.merge_collector {
                        collector.borrow_mut().collect_schema_input(&value)?;
                    }
                    values.push(SchemaValue { schema_id, value });
                }

                if !self.inner.next()? {
                    break;
                }
                if self.inner.key()? != Some(current_key.as_ref()) {
                    break;
                }
            }

            if values.is_empty() {
                continue;
            }

            values.reverse();
            let time_provider: &dyn TimeProvider = self.ttl_provider.time_provider();
            let (merge_plan, vlog_store, vlog_version) =
                (&mut self.merge_plan, &self.vlog_store, &self.vlog_version);
            let merged = merge_schema_values(values, merge_plan, Some(time_provider), |pointer| {
                vlog_store.read_pointer(vlog_version, pointer)
            })?
            .expect("non-empty compaction value chain produces a value");
            if let Some(collector) = &self.merge_collector {
                collector.borrow_mut().collect_schema_output(&merged)?;
            }
            self.current_key = Some(current_key);
            self.current_value = Some(KvValue::Decoded(merged));
            return Ok(());
        }
    }
}

impl<'a, I> KvIterator<'a> for SchemaAwareDeduplicatingIterator<I>
where
    I: KvIterator<'a>,
{
    fn seek(&mut self, target: &[u8]) -> Result<()> {
        self.inner.seek(target)?;
        self.collect_and_merge()
    }

    fn seek_to_first(&mut self) -> Result<()> {
        self.inner.seek_to_first()?;
        self.collect_and_merge()
    }

    fn next(&mut self) -> Result<bool> {
        self.collect_and_merge()?;
        Ok(self.valid())
    }

    fn valid(&self) -> bool {
        self.current_key.is_some()
    }

    fn key(&self) -> Result<Option<&[u8]>> {
        Ok(self.current_key.as_deref())
    }

    fn take_key(&mut self) -> Result<Option<Bytes>> {
        Ok(self.current_key.take())
    }

    fn take_value(&mut self) -> Result<Option<KvValue>> {
        Ok(self.current_value.take())
    }
}
