use crate::TimeProvider;
use crate::error::{Error, Result};
use crate::iterator::KvIterator;
use crate::row_merge::{SchemaMergePlan, SchemaValue, merge_schema_values};
use crate::schema::{Schema, SchemaManager};
use crate::ttl::TTLProvider;
use crate::r#type::KvValue;
use crate::vlog::{VlogMergeCollectorHandle, VlogStore, VlogVersion};
use bytes::Bytes;
use std::sync::Arc;

/// Deduplicates a merged scan or compaction stream while retaining each input
/// value's physical schema. It is used only when the selected work crosses a
/// schema barrier; compatible-only work keeps the lighter streaming path.
pub(crate) struct SchemaAwareDeduplicatingIterator<I> {
    inner: I,
    merge_plan: SchemaMergePlan<'static>,
    ttl_provider: Arc<TTLProvider>,
    vlog_store: Arc<VlogStore>,
    vlog_version: Arc<VlogVersion>,
    merge_collector: Option<VlogMergeCollectorHandle>,
    current_key: Option<Bytes>,
    current_value: Option<KvValue>,
    /// Key and physical-schema values accumulated while consuming one logical row.
    /// A block boundary may split the same-key chain, so this state must survive
    /// until the merged iterator has resumed and established the next key.
    collecting_key: Option<Bytes>,
    collecting_values: Vec<SchemaValue>,
    /// A terminal value remains a tombstone even when its TTL has expired:
    /// point reads stop at it before TTL filtering, so scans must not revive
    /// older values from behind it.
    collecting_terminal: bool,
    stopped_at_block_boundary: bool,
    resume_after_boundary: bool,
}

impl<I> SchemaAwareDeduplicatingIterator<I> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        inner: I,
        target_schema: Arc<Schema>,
        schema_manager: Arc<SchemaManager>,
        column_family_id: u8,
        ttl_provider: Arc<TTLProvider>,
        vlog_store: Arc<VlogStore>,
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
            vlog_store,
            vlog_version,
            merge_collector,
            current_key: None,
            current_value: None,
            collecting_key: None,
            collecting_values: Vec::new(),
            collecting_terminal: false,
            stopped_at_block_boundary: false,
            resume_after_boundary: false,
        }
    }

    fn source_num_columns(&self, schema_id: u64) -> Result<usize> {
        let schema = self.merge_plan.schema_manager().schema(schema_id)?;
        Ok(schema
            .num_columns_in_family(self.merge_plan.column_family_id())
            .unwrap_or_else(|| schema.num_columns()))
    }

    fn collect_value(&mut self, schema_id: u64, value: KvValue) -> Result<()> {
        let value = value.into_decoded(self.source_num_columns(schema_id)?)?;
        let terminal = value.is_terminal();
        // We still consume every physical value after a terminal so compaction
        // can account for VLOG inputs. They simply cannot participate in the
        // logical merge behind the newer terminal.
        let suppress_from_merge = self.collecting_terminal;
        if let Some(collector) = &self.merge_collector {
            collector.borrow_mut().collect_schema_input(&value)?;
        }
        if !self.ttl_provider.expired(&value.expired_at) && !suppress_from_merge {
            self.collecting_values
                .push(SchemaValue { schema_id, value });
        }
        self.collecting_terminal |= terminal;
        Ok(())
    }

    fn finish_current_key(&mut self) -> Result<()> {
        let current_key = self
            .collecting_key
            .take()
            .expect("schema-aware collector has a key when finishing");
        let mut values = std::mem::take(&mut self.collecting_values);
        self.collecting_terminal = false;
        if values.is_empty() {
            return Ok(());
        }
        values.reverse();
        let time_provider: &dyn TimeProvider = self.ttl_provider.time_provider();
        let (merge_plan, vlog_store, vlog_version) =
            (&mut self.merge_plan, &self.vlog_store, &self.vlog_version);
        let merged = merge_schema_values(values, merge_plan, Some(time_provider), |pointer| {
            vlog_store.read_pointer(vlog_version, pointer)
        })?
        .expect("non-empty schema-aware value chain produces a value");
        if let Some(collector) = &self.merge_collector {
            collector.borrow_mut().collect_schema_output(&merged)?;
        }
        self.current_key = Some(current_key);
        self.current_value = Some(KvValue::Decoded(merged));
        Ok(())
    }

    fn collect_and_merge<'b>(&mut self) -> Result<()>
    where
        I: KvIterator<'b>,
    {
        self.current_key = None;
        self.current_value = None;
        loop {
            if self.collecting_key.is_none() {
                if !self.inner.valid() {
                    if self.inner.stopped_at_block_boundary() {
                        self.stopped_at_block_boundary = true;
                    }
                    return Ok(());
                }
                self.collecting_key = self.inner.key()?.map(Bytes::copy_from_slice);
            }

            let schema_id = self.inner.current_schema_id().ok_or_else(|| {
                Error::InvalidState(
                    "schema-aware scan input is missing a physical schema tag".to_string(),
                )
            })?;
            let value = self
                .inner
                .take_value()?
                .expect("valid schema-aware iterator has a value");
            self.collect_value(schema_id, value)?;

            if !self.inner.next()? {
                if self.inner.stopped_at_block_boundary() {
                    // The next physical entry is unknown: it can still have this key.
                    self.stopped_at_block_boundary = true;
                    return Ok(());
                }
                self.finish_current_key()?;
                if self.current_key.is_some() {
                    return Ok(());
                }
                continue;
            }
            if self.inner.key()? == self.collecting_key.as_deref() {
                continue;
            }
            self.finish_current_key()?;
            if self.current_key.is_some() {
                return Ok(());
            }
        }
    }
}

impl<'a, I> KvIterator<'a> for SchemaAwareDeduplicatingIterator<I>
where
    I: KvIterator<'a>,
{
    fn seek(&mut self, target: &[u8]) -> Result<()> {
        self.current_key = None;
        self.current_value = None;
        self.collecting_key = None;
        self.collecting_values.clear();
        self.collecting_terminal = false;
        self.stopped_at_block_boundary = false;
        self.resume_after_boundary = false;
        self.inner.seek(target)?;
        self.collect_and_merge()
    }

    fn seek_to_first(&mut self) -> Result<()> {
        self.current_key = None;
        self.current_value = None;
        self.collecting_key = None;
        self.collecting_values.clear();
        self.collecting_terminal = false;
        self.stopped_at_block_boundary = false;
        self.resume_after_boundary = false;
        self.inner.seek_to_first()?;
        self.collect_and_merge()
    }

    fn next(&mut self) -> Result<bool> {
        if self.stopped_at_block_boundary {
            return Ok(false);
        }
        if self.resume_after_boundary {
            self.resume_after_boundary = false;
            if !self.inner.next()? {
                if self.inner.stopped_at_block_boundary() {
                    self.stopped_at_block_boundary = true;
                    return Ok(false);
                }
                if self.collecting_key.is_some() {
                    self.finish_current_key()?;
                }
                return Ok(self.valid());
            }
            if self.collecting_key.is_some() && self.inner.key()? != self.collecting_key.as_deref()
            {
                self.finish_current_key()?;
                if self.valid() {
                    return Ok(true);
                }
            }
        }
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

    fn set_stop_at_block_boundary(&mut self, enabled: bool) {
        self.stopped_at_block_boundary = false;
        self.resume_after_boundary = false;
        self.inner.set_stop_at_block_boundary(enabled);
    }

    fn clear_stop_at_block_boundary(&mut self) {
        if self.stopped_at_block_boundary {
            self.stopped_at_block_boundary = false;
            self.resume_after_boundary = true;
        }
        self.inner.clear_stop_at_block_boundary();
    }

    fn stopped_at_block_boundary(&self) -> bool {
        self.stopped_at_block_boundary
    }
}
