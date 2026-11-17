//! Builds online iterators over active and immutable memtable snapshots.

use super::{ActiveMemtable, MemtableManager};
use crate::db_state::DbState;
use crate::error::Error::InvalidState;
use crate::error::Result;
use crate::iterator::{
    ColumnMaskingIterator, KvIterator, SchemaEvolvingIterator, SchemaTaggedIterator,
    selects_all_columns,
};
use crate::memtable::{Memtable, MemtableImpl, SkiplistMemtable};
use crate::schema::Schema;
use crate::r#type::{KvValue, ValueType};
use bytes::Bytes;
use std::ops::Range;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

type DynKvIterator = Box<dyn for<'a> KvIterator<'a>>;

#[derive(Clone)]
enum MemtableScanSource {
    Active(Arc<RwLock<ActiveMemtable>>),
    Immutable(Arc<MemtableImpl>),
}

/// Keeps an immutable skiplist entry alive for `Bytes` views that outlive a scan cursor.
struct ImmutableSkiplistEntry {
    memtable: Arc<MemtableImpl>,
    range: Range<usize>,
}

impl AsRef<[u8]> for ImmutableSkiplistEntry {
    fn as_ref(&self) -> &[u8] {
        let MemtableImpl::Skiplist(skiplist_memtable) = self.memtable.as_ref() else {
            unreachable!("immutable skiplist entry must keep a skiplist memtable")
        };
        skiplist_memtable
            .entry_bytes(&self.range)
            .expect("immutable skiplist entry must remain valid")
    }
}

struct SkiplistScanCursor {
    source: MemtableScanSource,
    end_bound_exclusive: Option<Bytes>,
    next_node: Option<u32>,
    current_key: Option<Bytes>,
    current_value: Option<Bytes>,
}

impl SkiplistScanCursor {
    fn new(source: MemtableScanSource, end_bound_exclusive: Option<Bytes>) -> Self {
        Self {
            source,
            end_bound_exclusive,
            next_node: None,
            current_key: None,
            current_value: None,
        }
    }

    fn read_skiplist_node(&self, node: u32) -> Result<Option<(Bytes, Bytes, Option<u32>)>> {
        let copy_active_entry = |memtable: &SkiplistMemtable| {
            let (range, key_len) = memtable.node_entry_range(node)?;
            let entry = Bytes::copy_from_slice(memtable.entry_bytes(&range)?);
            Some((
                entry.slice(..key_len),
                entry.slice(key_len..),
                memtable.next_node_offset(node),
            ))
        };
        match &self.source {
            MemtableScanSource::Active(active) => {
                let active = active.read().unwrap();
                let Some(memtable) = active.readable_memtable() else {
                    return Ok(None);
                };
                match memtable {
                    MemtableImpl::Skiplist(skiplist_memtable) => {
                        Ok(copy_active_entry(skiplist_memtable))
                    }
                    _ => Err(InvalidState(
                        "skiplist iterator expected skiplist active memtable".to_string(),
                    )),
                }
            }
            MemtableScanSource::Immutable(memtable) => match memtable.as_ref() {
                MemtableImpl::Skiplist(skiplist_memtable) => {
                    let Some((range, key_len)) = skiplist_memtable.node_entry_range(node) else {
                        return Ok(None);
                    };
                    let entry = Bytes::from_owner(ImmutableSkiplistEntry {
                        memtable: Arc::clone(memtable),
                        range,
                    });
                    Ok(Some((
                        entry.slice(..key_len),
                        entry.slice(key_len..),
                        skiplist_memtable.next_node_offset(node),
                    )))
                }
                _ => Err(InvalidState(
                    "skiplist iterator expected skiplist immutable memtable".to_string(),
                )),
            },
        }
    }

    fn skiplist_start_node(&self, seek_target: Option<&[u8]>) -> Result<Option<u32>> {
        let find_start = |memtable: &SkiplistMemtable| {
            if let Some(target) = seek_target {
                memtable.lower_bound_node_offset(target)
            } else {
                memtable.first_node_offset()
            }
        };
        match &self.source {
            MemtableScanSource::Active(active) => {
                let active = active.read().unwrap();
                let Some(memtable) = active.readable_memtable() else {
                    return Ok(None);
                };
                match memtable {
                    MemtableImpl::Skiplist(skiplist_memtable) => Ok(find_start(skiplist_memtable)),
                    _ => Err(InvalidState(
                        "skiplist iterator expected skiplist active memtable".to_string(),
                    )),
                }
            }
            MemtableScanSource::Immutable(memtable) => match memtable.as_ref() {
                MemtableImpl::Skiplist(skiplist_memtable) => Ok(find_start(skiplist_memtable)),
                _ => Err(InvalidState(
                    "skiplist iterator expected skiplist immutable memtable".to_string(),
                )),
            },
        }
    }

    fn advance(&mut self) -> Result<bool> {
        let Some(node) = self.next_node else {
            self.current_key = None;
            self.current_value = None;
            return Ok(false);
        };
        let Some((key, value, next_node)) = self.read_skiplist_node(node)? else {
            self.next_node = None;
            self.current_key = None;
            self.current_value = None;
            return Ok(false);
        };
        self.next_node = next_node;
        if let Some(end) = self.end_bound_exclusive.as_deref()
            && key.as_ref() >= end
        {
            self.next_node = None;
            self.current_key = None;
            self.current_value = None;
            return Ok(false);
        }
        self.current_key = Some(key);
        self.current_value = Some(value);
        Ok(true)
    }

    fn seek_inner(&mut self, target: Option<&[u8]>) -> Result<()> {
        self.next_node = self.skiplist_start_node(target)?;
        let _ = self.advance()?;
        Ok(())
    }
}

impl<'a> KvIterator<'a> for SkiplistScanCursor {
    fn seek(&mut self, target: &[u8]) -> Result<()> {
        self.seek_inner(Some(target))
    }

    fn seek_to_first(&mut self) -> Result<()> {
        self.seek_inner(None)
    }

    fn next(&mut self) -> Result<bool> {
        self.advance()
    }

    fn valid(&self) -> bool {
        self.current_key.is_some() && self.current_value.is_some()
    }

    fn key(&self) -> Result<Option<&[u8]>> {
        Ok(self.current_key.as_deref())
    }

    fn take_key(&mut self) -> Result<Option<Bytes>> {
        Ok(self.current_key.take())
    }

    fn take_value(&mut self) -> Result<Option<KvValue>> {
        Ok(self.current_value.take().map(KvValue::encoded))
    }
}

struct MemtableScanIterator {
    source: MemtableScanSource,
    /// Scan lower bound already encoded in DB key space.
    start_bound_inclusive: Option<Bytes>,
    /// Scan upper bound already encoded in DB key space.
    end_bound_exclusive: Option<Bytes>,
    /// Optional projected-row visibility filter used only on the collect path.
    /// When present, collected rows that are provably invisible for the projection
    /// (for example all selected columns are empty/Delete) do not count toward `max_rows`.
    row_filter: Option<MemtableRowFilter>,
    /// The caller-requested visible-row limit for this memtable layer.
    /// This is only meaningful on paths that materialize collected entries eagerly.
    max_rows: Option<usize>,
    /// Shared allowance flowing from newer collected memtables to older collected memtables.
    /// Each invisible shadowing row found in an upper collected layer increments this allowance,
    /// so lower collected layers fetch extra candidates before the final DbIterator dedup pass.
    shadow_allowance: Option<Arc<AtomicUsize>>,
    /// Marks the first collected memtable iterator in the chain so it can reset the shared
    /// allowance before a seek starts walking newer-to-older collected layers.
    shadow_allowance_root: bool,
    entries: Vec<(Bytes, Bytes)>,
    next_index: usize,
    skiplist_iter: Option<SkiplistScanCursor>,
    current_key: Option<Bytes>,
    current_value: Option<Bytes>,
}

#[derive(Clone)]
struct MemtableRowFilter {
    num_columns: usize,
    selected_columns: Option<Vec<usize>>,
}

fn build_memtable_row_filter(
    source_schema: &Arc<Schema>,
    target_schema_version: u64,
    column_family_id: u8,
    selected_columns: Option<&[usize]>,
) -> Option<MemtableRowFilter> {
    if source_schema.version() != target_schema_version {
        return None;
    }
    Some(MemtableRowFilter {
        num_columns: source_schema
            .num_columns_in_family(column_family_id)
            .unwrap_or(0),
        selected_columns: selected_columns.map(|columns| columns.to_vec()),
    })
}

impl MemtableScanIterator {
    fn for_active(
        active: Arc<RwLock<ActiveMemtable>>,
        start_bound_inclusive: Option<Bytes>,
        end_bound_exclusive: Option<Bytes>,
        row_filter: Option<MemtableRowFilter>,
        max_rows: Option<usize>,
        shadow_allowance: Option<Arc<AtomicUsize>>,
        shadow_allowance_root: bool,
    ) -> Self {
        Self {
            source: MemtableScanSource::Active(active),
            start_bound_inclusive,
            end_bound_exclusive,
            row_filter,
            max_rows,
            shadow_allowance,
            shadow_allowance_root,
            entries: Vec::new(),
            next_index: 0,
            skiplist_iter: None,
            current_key: None,
            current_value: None,
        }
    }

    fn for_immutable(
        memtable: Arc<MemtableImpl>,
        start_bound_inclusive: Option<Bytes>,
        end_bound_exclusive: Option<Bytes>,
        row_filter: Option<MemtableRowFilter>,
        max_rows: Option<usize>,
        shadow_allowance: Option<Arc<AtomicUsize>>,
        shadow_allowance_root: bool,
    ) -> Self {
        Self {
            source: MemtableScanSource::Immutable(memtable),
            start_bound_inclusive,
            end_bound_exclusive,
            row_filter,
            max_rows,
            shadow_allowance,
            shadow_allowance_root,
            entries: Vec::new(),
            next_index: 0,
            skiplist_iter: None,
            current_key: None,
            current_value: None,
        }
    }

    fn reload_collected_entries(
        &mut self,
        start_bound_inclusive: Option<&[u8]>,
        end_bound_exclusive: Option<&[u8]>,
    ) -> Result<()> {
        if self.shadow_allowance_root
            && let Some(shadow_allowance) = self.shadow_allowance.as_ref()
        {
            shadow_allowance.store(0, Ordering::Relaxed);
        }
        let effective_max_rows = if self.row_filter.is_some() {
            self.max_rows
        } else {
            None
        };
        self.skiplist_iter = None;
        self.entries = match &self.source {
            MemtableScanSource::Active(active) => {
                let mut active = active.write().unwrap();
                // Iterator construction and the first seek are separate operations. Seal again
                // while collecting so entries appended between them cannot be rewritten in place
                // after their zero-copy `Bytes` views escape this lock.
                active.seal_read_snapshot_data();
                let Some(memtable) = active.readable_memtable() else {
                    self.entries.clear();
                    self.current_key = None;
                    self.current_value = None;
                    self.next_index = 0;
                    return Ok(());
                };
                collect_limited_entries_from_memtable(
                    memtable,
                    start_bound_inclusive,
                    end_bound_exclusive,
                    self.row_filter.as_ref(),
                    effective_max_rows,
                    self.shadow_allowance.as_ref(),
                )?
            }
            MemtableScanSource::Immutable(memtable) => collect_limited_entries_from_memtable(
                memtable.as_ref(),
                start_bound_inclusive,
                end_bound_exclusive,
                self.row_filter.as_ref(),
                effective_max_rows,
                self.shadow_allowance.as_ref(),
            )?,
        };
        self.prime_current();
        Ok(())
    }

    fn reload_skiplist_stream(&mut self, start_bound_inclusive: Option<&[u8]>) -> Result<()> {
        self.entries.clear();
        self.next_index = 0;
        self.current_key = None;
        self.current_value = None;
        let mut skiplist_iter =
            SkiplistScanCursor::new(self.source.clone(), self.end_bound_exclusive.clone());
        if let Some(target) = start_bound_inclusive {
            skiplist_iter.seek(target)?;
        } else {
            skiplist_iter.seek_to_first()?;
        }
        self.skiplist_iter = Some(skiplist_iter);
        Ok(())
    }

    fn reload_entries(&mut self, seek_target: Option<&[u8]>) -> Result<()> {
        let effective_start = match (self.start_bound_inclusive.as_ref(), seek_target) {
            (Some(bound), Some(target)) => {
                if bound.as_ref() >= target {
                    Some(bound.clone())
                } else {
                    Some(Bytes::copy_from_slice(target))
                }
            }
            (Some(bound), None) => Some(bound.clone()),
            (None, Some(target)) => Some(Bytes::copy_from_slice(target)),
            (None, None) => None,
        };
        let start_bound_inclusive = effective_start.as_deref();
        let end_bound_exclusive = self.end_bound_exclusive.clone();
        let end_bound_exclusive_ref = end_bound_exclusive.as_deref();
        let source_is_immutable_skiplist = matches!(
            &self.source,
            MemtableScanSource::Immutable(memtable)
                if matches!(memtable.as_ref(), MemtableImpl::Skiplist(_))
        );
        if source_is_immutable_skiplist {
            self.reload_skiplist_stream(start_bound_inclusive)
        } else {
            self.reload_collected_entries(start_bound_inclusive, end_bound_exclusive_ref)
        }
    }

    fn prime_current(&mut self) {
        if let Some((key, value)) = self.entries.first().cloned() {
            self.current_key = Some(key);
            self.current_value = Some(value);
            self.next_index = 1;
        } else {
            self.current_key = None;
            self.current_value = None;
            self.next_index = 0;
        }
    }
}

fn row_counts_toward_max_rows(
    encoded_value: &Bytes,
    row_filter: &MemtableRowFilter,
) -> Result<bool> {
    let decoded = KvValue::encoded(encoded_value.clone()).into_decoded(row_filter.num_columns)?;
    let has_visible_column = if let Some(selected_columns) = row_filter.selected_columns.as_ref() {
        selected_columns.iter().any(|&index| {
            decoded.columns().get(index).is_some_and(|column| {
                column
                    .as_ref()
                    .is_some_and(|column| column.value_type() != &ValueType::Delete)
            })
        })
    } else {
        decoded.columns().iter().any(|column| {
            column
                .as_ref()
                .is_some_and(|column| column.value_type() != &ValueType::Delete)
        })
    };
    Ok(has_visible_column)
}

fn collect_limited_entries_from_iter<'a, I>(
    iter: &mut I,
    seek_target: Option<&[u8]>,
    end_bound_exclusive: Option<&[u8]>,
    row_filter: Option<&MemtableRowFilter>,
    max_rows: Option<usize>,
    shadow_allowance: Option<&Arc<AtomicUsize>>,
) -> Result<Vec<(Bytes, Bytes)>>
where
    I: KvIterator<'a>,
{
    if let Some(target) = seek_target {
        iter.seek(target)?;
    } else {
        iter.seek_to_first()?;
    }
    let mut entries = Vec::new();
    let mut visible_rows = 0usize;
    // The local max_rows budget starts from the caller's requested visible-row cap,
    // then inherits any extra allowance already earned by newer collected memtables.
    // That inherited allowance compensates for newer invisible shadowing rows that can
    // suppress same-key values in this older collected layer during the final dedup pass.
    let effective_max_rows = max_rows.map(|max_rows| {
        max_rows + shadow_allowance.map_or(0, |allowance| allowance.load(Ordering::Relaxed))
    });
    while iter.next()? {
        if let Some((key, kv_value)) = iter.take_current()? {
            if let Some(end) = end_bound_exclusive
                && key.as_ref() >= end
            {
                break;
            }
            let encoded_value = kv_value.unwrap_encoded();
            // Visibility decoding serves only the local row budget. The final iterator still
            // decodes and filters these values after deduplication.
            let Some(max_rows) = effective_max_rows else {
                entries.push((key, encoded_value));
                continue;
            };
            let counts_toward_limit = if let Some(row_filter) = row_filter {
                row_counts_toward_max_rows(&encoded_value, row_filter)?
            } else {
                true
            };
            entries.push((key, encoded_value));
            if counts_toward_limit {
                visible_rows += 1;
            } else {
                // This row is already provably invisible for the projected read, but it can still
                // shadow an older same-key value. Propagate one extra slot to lower collected
                // memtables so they can fetch the next candidate instead of being truncated early.
                if let Some(shadow_allowance) = shadow_allowance {
                    shadow_allowance.fetch_add(1, Ordering::Relaxed);
                }
            }
            if visible_rows >= max_rows {
                break;
            }
        }
    }
    Ok(entries)
}

fn collect_limited_entries_from_memtable(
    memtable: &MemtableImpl,
    seek_target: Option<&[u8]>,
    end_bound_exclusive: Option<&[u8]>,
    row_filter: Option<&MemtableRowFilter>,
    max_rows: Option<usize>,
    shadow_allowance: Option<&Arc<AtomicUsize>>,
) -> Result<Vec<(Bytes, Bytes)>> {
    match memtable {
        MemtableImpl::Hash(hash_memtable) => {
            let mut iter = hash_memtable.iter_with_bounds(seek_target, end_bound_exclusive);
            collect_limited_entries_from_iter(
                &mut iter,
                seek_target,
                end_bound_exclusive,
                row_filter,
                max_rows,
                shadow_allowance,
            )
        }
        _ => {
            let mut iter = memtable.iter();
            collect_limited_entries_from_iter(
                &mut iter,
                seek_target,
                end_bound_exclusive,
                row_filter,
                max_rows,
                shadow_allowance,
            )
        }
    }
}

impl<'a> KvIterator<'a> for MemtableScanIterator {
    fn seek(&mut self, target: &[u8]) -> Result<()> {
        self.reload_entries(Some(target))
    }

    fn seek_to_first(&mut self) -> Result<()> {
        self.reload_entries(None)
    }

    fn next(&mut self) -> Result<bool> {
        if let Some(iter) = self.skiplist_iter.as_mut() {
            return iter.next();
        }
        if let Some((key, value)) = self.entries.get(self.next_index).cloned() {
            self.current_key = Some(key);
            self.current_value = Some(value);
            self.next_index += 1;
            Ok(true)
        } else {
            self.current_key = None;
            self.current_value = None;
            Ok(false)
        }
    }

    fn valid(&self) -> bool {
        if let Some(iter) = self.skiplist_iter.as_ref() {
            return iter.valid();
        }
        self.current_key.is_some() && self.current_value.is_some()
    }

    fn key(&self) -> Result<Option<&[u8]>> {
        if let Some(iter) = self.skiplist_iter.as_ref() {
            return iter.key();
        }
        Ok(self.current_key.as_deref())
    }

    fn take_key(&mut self) -> Result<Option<Bytes>> {
        if let Some(iter) = self.skiplist_iter.as_mut() {
            return iter.take_key();
        }
        Ok(self.current_key.take())
    }

    fn take_value(&mut self) -> Result<Option<KvValue>> {
        if let Some(iter) = self.skiplist_iter.as_mut() {
            return iter.take_value();
        }
        Ok(self.current_value.take().map(KvValue::encoded))
    }
}

impl MemtableManager {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn scan_memtable_iterators_with_snapshot(
        &self,
        snapshot: Arc<DbState>,
        target_schema: Arc<Schema>,
        column_family_id: u8,
        selected_columns: Option<&[usize]>,
        start_bound_inclusive: Option<Bytes>,
        end_bound_exclusive: Option<Bytes>,
        max_rows: Option<usize>,
        schema_aware: bool,
    ) -> Result<Vec<DynKvIterator>> {
        let mut iterators: Vec<DynKvIterator> = Vec::new();
        let target_num_columns = target_schema
            .num_columns_in_family(column_family_id)
            .unwrap_or(0);
        let max_rows = (!schema_aware).then_some(max_rows).flatten();
        let selected_columns = (!schema_aware).then_some(selected_columns).flatten();
        let shared_shadow_allowance = max_rows.map(|_| Arc::new(AtomicUsize::new(0)));
        let mut first_collecting_iterator = true;
        let mut deeper_collected_limits_allowed = true;
        if let Some(active) = &snapshot.active {
            let source_schema = {
                let mut active_guard = active.write().unwrap();
                active_guard.seal_read_snapshot_data();
                Arc::clone(&active_guard.schema)
            };
            let row_filter = max_rows.and_then(|_| {
                build_memtable_row_filter(
                    &source_schema,
                    target_schema.version(),
                    column_family_id,
                    selected_columns,
                )
            });
            if max_rows.is_some() && row_filter.is_none() {
                deeper_collected_limits_allowed = false;
            }
            let shadow_allowance = row_filter
                .as_ref()
                .and_then(|_| shared_shadow_allowance.as_ref().map(Arc::clone));
            let shadow_allowance_root = shadow_allowance.is_some() && first_collecting_iterator;
            if shadow_allowance.is_some() {
                first_collecting_iterator = false;
            }
            let iter: DynKvIterator = Box::new(MemtableScanIterator::for_active(
                Arc::clone(active),
                start_bound_inclusive.clone(),
                end_bound_exclusive.clone(),
                row_filter,
                max_rows,
                shadow_allowance,
                shadow_allowance_root,
            ));
            let iter: DynKvIterator = if schema_aware {
                Box::new(SchemaTaggedIterator::new(iter, source_schema.version()))
            } else if source_schema.version() == target_schema.version() {
                iter
            } else {
                Box::new(SchemaEvolvingIterator::new(
                    iter,
                    Arc::clone(&source_schema),
                    Arc::clone(&target_schema),
                    Arc::clone(&self.schema_manager),
                    column_family_id,
                ))
            };
            // Preserve encoded rows for full projection, after schema evolution.
            let iter: DynKvIterator = if let Some(columns) = selected_columns
                && !selects_all_columns(columns, target_num_columns)
            {
                Box::new(ColumnMaskingIterator::new(
                    iter,
                    target_num_columns,
                    columns,
                ))
            } else {
                iter
            };
            iterators.push(iter);
        }
        for immutable in snapshot.immutables.iter().rev() {
            let skiplist_streaming =
                matches!(immutable.memtable.as_ref(), MemtableImpl::Skiplist(_));
            let row_filter =
                if max_rows.is_some() && deeper_collected_limits_allowed && !skiplist_streaming {
                    build_memtable_row_filter(
                        &immutable.schema,
                        target_schema.version(),
                        column_family_id,
                        selected_columns,
                    )
                } else {
                    None
                };
            let row_filter_missing = row_filter.is_none();
            let shadow_allowance = if skiplist_streaming {
                None
            } else {
                row_filter
                    .as_ref()
                    .and_then(|_| shared_shadow_allowance.as_ref().map(Arc::clone))
            };
            let shadow_allowance_root = shadow_allowance.is_some() && first_collecting_iterator;
            if shadow_allowance.is_some() {
                first_collecting_iterator = false;
            }
            let iter: DynKvIterator = Box::new(MemtableScanIterator::for_immutable(
                Arc::clone(&immutable.memtable),
                start_bound_inclusive.clone(),
                end_bound_exclusive.clone(),
                row_filter,
                if deeper_collected_limits_allowed && !skiplist_streaming {
                    max_rows
                } else {
                    None
                },
                shadow_allowance,
                shadow_allowance_root,
            ));
            let iter: DynKvIterator = if schema_aware {
                Box::new(SchemaTaggedIterator::new(iter, immutable.schema.version()))
            } else if immutable.schema.version() == target_schema.version() {
                iter
            } else {
                Box::new(SchemaEvolvingIterator::new(
                    iter,
                    Arc::clone(&immutable.schema),
                    Arc::clone(&target_schema),
                    Arc::clone(&self.schema_manager),
                    column_family_id,
                ))
            };
            let iter: DynKvIterator = if let Some(columns) = selected_columns
                && !selects_all_columns(columns, target_num_columns)
            {
                Box::new(ColumnMaskingIterator::new(
                    iter,
                    target_num_columns,
                    columns,
                ))
            } else {
                iter
            };
            iterators.push(iter);
            if skiplist_streaming || (max_rows.is_some() && row_filter_missing) {
                deeper_collected_limits_allowed = false;
            }
        }
        Ok(iterators)
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/memtable/scan.rs"]
mod tests;
