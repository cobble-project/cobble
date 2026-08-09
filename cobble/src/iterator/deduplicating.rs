//! Deduplicating iterator that merges values with the same key.
//!
//! This module provides a wrapper iterator that merges duplicate keys
//! by combining their values according to the merge semantics defined
//! in the `Value` type.

use crate::error::Result;
use crate::iterator::KvIterator;
use crate::schema::{DEFAULT_COLUMN_FAMILY_ID, Schema};
use crate::ttl::TTLProvider;
use crate::r#type::{Column, KvValue, Value, key_column_family};
use bytes::Bytes;
use std::sync::Arc;

/// Callback type for column merges. The callback is invoked for every pair of merged columns,
/// with the older column (if any) and the newer column (if any). And also the oldest first column
/// is guaranteed to be called first as well, as callback(None, oldest_column) before any
/// newer column is merged.
type MergeCallback = Box<dyn FnMut(Option<&Column>, Option<&Column>)>;
/// Callback invoked when a value is expired and dropped during compaction, so the caller can
/// collect VLOG removal deltas for separated-value columns. Returning an error aborts the
/// compaction - a corrupt expired value must not be silently dropped without updating VLOG
/// reference counts.
type ExpiredCallback = Box<dyn FnMut(&Value) -> Result<()>>;

/// Boundary handling for the deduplicating wrapper.
///
/// This iterator has to preserve two separate phases around a physical block boundary:
/// 1. We may discover the boundary while we are still finishing the merged row for the current key.
///    In that case we must return that row first, then surface the stop on the next `next()`.
/// 2. After the stop has been surfaced, callers must explicitly clear it. Only after that clear
///    should the next `next()` advance the inner iterator once to resume from the boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BoundaryState {
    /// No pending boundary work.
    None,
    /// A boundary was discovered while building the current merged row. The current row should be
    /// returned now, and the next `next()` must report a stop before any resume happens.
    PendingStop,
    /// The stop has already been surfaced to callers. The iterator must keep returning `false`
    /// until callers explicitly clear the stop.
    Stopped,
    /// Callers cleared the surfaced stop. The next `next()` should advance the inner iterator once
    /// to move past the saved boundary and resume normal iteration.
    ReadyToResume,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OutputMode {
    /// Query and point-read consumers need a logical value. A lone merge operand must therefore
    /// be resolved into a materialized `Value` before it leaves this iterator.
    Materialize,
    /// Flush and compaction are writing another versioned SST. If only one non-terminal encoded
    /// value survives, forwarding its original bytes preserves the merge operand for future reads
    /// and avoids a decode-then-encode cycle.
    PreserveSingleValueForSst,
}

/// A deduplicating iterator that wraps another iterator and merges
/// values with the same key.
///
/// When multiple entries have the same key, the values are merged
/// according to the column merge semantics:
/// - Put/Delete replaces the previous value
/// - Merge concatenates data to the previous value
///
/// The wrapped iterator must already produce keys in sorted order
/// (typically a `MergingIterator`), with entries from newer sources
/// appearing before entries from older sources when keys are equal.
pub struct DeduplicatingIterator<I> {
    /// The underlying iterator (typically a MergingIterator).
    inner: I,
    /// When present, callers already know the exact column width (for example projected read
    /// chains and compaction). When absent, we derive it from the key's column family.
    num_columns: Option<usize>,
    /// Current merged key (if valid).
    current_key: Option<Bytes>,
    /// Current merged value (if valid).
    current_value: Option<KvValue>,
    /// Boundary lifecycle for the current merged-key position.
    /// This lets us defer surfacing a stop until after the current merged row
    /// has been returned, and then require an explicit clear before resuming.
    boundary_state: BoundaryState,
    /// TTL provider to evaluate expiration.
    ttl_provider: Arc<TTLProvider>,
    /// Callback invoked for every merged column pair (older, newer).
    on_merge: Option<MergeCallback>,
    /// Callback invoked when a value is expired and dropped, for VLOG delta collection.
    on_expired: Option<ExpiredCallback>,
    /// Whether to allow terminal fast-path that skips collecting older versions.
    allow_terminal_shortcut: bool,
    /// Representation contract for a lone non-terminal value after deduplication.
    output_mode: OutputMode,
    /// Merge operators used for column merge semantics.
    schema: Arc<Schema>,
}

/// Collects a value into the values vector or selects it as the final value.
/// This function checks for expiration and terminal status.
/// Takes ownership of the KvValue to avoid unnecessary copies.
#[allow(clippy::too_many_arguments)]
fn collect_value(
    value: KvValue,
    num_columns: usize,
    ttl_provider: &TTLProvider,
    allow_terminal_shortcut: bool,
    values: &mut Vec<KvValue>,
    selected_value: &mut Option<KvValue>,
    stop_collecting: &mut bool,
    on_expired: &mut Option<ExpiredCallback>,
) -> Result<()> {
    let expired_at = value.expired_at()?;
    if ttl_provider.expired(&expired_at) {
        // The value is expired and will be dropped. If a VLOG collection callback is
        // present, decode the value and forward it so removal deltas can be collected.
        // A decode failure is an error: the value would be deleted without updating VLOG
        // reference counts, leaking separated-value entries.
        if let Some(callback) = on_expired.as_deref_mut() {
            let decoded = value.into_decoded(num_columns)?;
            callback(&decoded)?;
        }
        return Ok(());
    }
    let is_terminal = value.is_terminal(num_columns)?;
    if allow_terminal_shortcut && selected_value.is_none() && values.is_empty() && is_terminal {
        *selected_value = Some(value);
        *stop_collecting = true;
        return Ok(());
    }
    values.push(value);
    if is_terminal {
        *stop_collecting = true;
    }
    Ok(())
}

impl<I> DeduplicatingIterator<I> {
    /// Creates a new `DeduplicatingIterator` wrapping the given iterator.
    ///
    /// # Arguments
    /// * `inner` - The underlying iterator to wrap.
    /// * `num_columns` - Known number of columns in the value schema, if the caller already has it.
    pub fn new(
        inner: I,
        num_columns: Option<usize>,
        ttl_provider: Arc<TTLProvider>,
        on_merge: Option<MergeCallback>,
        schema: Arc<Schema>,
    ) -> Self {
        Self::new_with_output_mode(
            inner,
            num_columns,
            ttl_provider,
            on_merge,
            None,
            schema,
            OutputMode::Materialize,
        )
    }

    /// Creates an iterator for SST construction.
    ///
    /// Unlike a read result, an SST may retain an unresolved merge operand. This constructor lets
    /// the iterator forward a lone encoded operand when no merge callback needs decoded columns.
    pub(crate) fn new_for_sst_build(
        inner: I,
        num_columns: Option<usize>,
        ttl_provider: Arc<TTLProvider>,
        on_merge: Option<MergeCallback>,
        on_expired: Option<ExpiredCallback>,
        schema: Arc<Schema>,
    ) -> Self {
        Self::new_with_output_mode(
            inner,
            num_columns,
            ttl_provider,
            on_merge,
            on_expired,
            schema,
            OutputMode::PreserveSingleValueForSst,
        )
    }

    fn new_with_output_mode(
        inner: I,
        num_columns: Option<usize>,
        ttl_provider: Arc<TTLProvider>,
        on_merge: Option<MergeCallback>,
        on_expired: Option<ExpiredCallback>,
        schema: Arc<Schema>,
        output_mode: OutputMode,
    ) -> Self {
        let allow_terminal_shortcut = on_merge.is_none();
        Self {
            inner,
            num_columns,
            current_key: None,
            current_value: None,
            boundary_state: BoundaryState::None,
            ttl_provider,
            on_merge,
            on_expired,
            allow_terminal_shortcut,
            output_mode,
            schema,
        }
    }

    fn num_columns_for_key(&self, key: &[u8]) -> usize {
        if let Some(num_columns) = self.num_columns {
            return num_columns;
        }
        let column_family_id = key_column_family(key).unwrap_or(DEFAULT_COLUMN_FAMILY_ID);
        self.schema
            .num_columns_in_family(column_family_id)
            .filter(|num_columns| *num_columns > 0)
            .unwrap_or_else(|| self.schema.num_columns())
    }

    /// Collects all values with the same key and merges them.
    ///
    /// This method consumes entries from the inner iterator until
    /// the key changes, merging all values along the way.
    ///
    /// The iterator is expected to return entries in order where newer entries
    /// come before older entries for the same key. We collect all values and
    /// then merge from oldest to newest, so that newer values override older ones.
    fn collect_and_merge<'a>(&mut self) -> Result<()>
    where
        I: KvIterator<'a>,
    {
        let allow_terminal_shortcut = self.allow_terminal_shortcut;
        loop {
            if !self.inner.valid() {
                self.current_key = None;
                self.current_value = None;
                if self.inner.stopped_at_block_boundary() {
                    self.boundary_state = BoundaryState::Stopped;
                }
                return Ok(());
            }

            // Take the first key-value pair
            let Some((current_key, first_value)) = self.inner.take_current()? else {
                self.current_key = None;
                self.current_value = None;
                if self.inner.stopped_at_block_boundary() {
                    self.boundary_state = BoundaryState::Stopped;
                }
                return Ok(());
            };

            let mut values: Vec<KvValue> = Vec::new();
            let mut selected_value: Option<KvValue> = None;
            let mut stop_collecting = false;
            let num_columns = self.num_columns_for_key(current_key.as_ref());

            collect_value(
                first_value,
                num_columns,
                &self.ttl_provider,
                allow_terminal_shortcut,
                &mut values,
                &mut selected_value,
                &mut stop_collecting,
                &mut self.on_expired,
            )?;

            // Advance to next entry and check for same key
            while {
                let advanced = self.inner.next()?;
                if !advanced && self.inner.stopped_at_block_boundary() {
                    self.boundary_state = BoundaryState::PendingStop;
                }
                advanced
            } {
                let Some(next_key) = self.inner.key()? else {
                    break;
                };
                if next_key != current_key.as_ref() {
                    // Different key, stop collecting
                    break;
                }
                if stop_collecting && allow_terminal_shortcut {
                    continue;
                }

                // Same key, take the value
                if let Some(next_kv_value) = self.inner.take_value()? {
                    collect_value(
                        next_kv_value,
                        num_columns,
                        &self.ttl_provider,
                        allow_terminal_shortcut,
                        &mut values,
                        &mut selected_value,
                        &mut stop_collecting,
                        &mut self.on_expired,
                    )?;
                }
            }

            if let Some(value) = selected_value {
                self.current_key = Some(current_key);
                self.current_value = Some(value);
                return Ok(());
            }

            if values.is_empty() {
                if self.boundary_state == BoundaryState::PendingStop {
                    self.boundary_state = BoundaryState::Stopped;
                    self.current_key = None;
                    self.current_value = None;
                    return Ok(());
                }
                // All versions for this key are expired; continue to the next key.
                continue;
            }

            if self.output_mode == OutputMode::PreserveSingleValueForSst
                && self.on_merge.is_none()
                && values.len() == 1
            {
                // No older value participates in this key, so preserving the operand is equivalent
                // to rewriting it after materialization while avoiding both codec passes.
                self.current_key = Some(current_key);
                self.current_value = values.pop();
                return Ok(());
            }

            let column_family_id =
                key_column_family(current_key.as_ref()).unwrap_or(DEFAULT_COLUMN_FAMILY_ID);
            if let Some(callback) = self.on_merge.as_deref_mut() {
                // Keep callback semantics and memory usage pairwise: decode each value only when
                // it is consumed by the callback merge path.
                let mut values_iter = values.into_iter().rev();
                let mut merged_value = values_iter
                    .next()
                    .expect("values is non-empty")
                    .into_decoded(num_columns)?;
                // The first column is invoked with callback(None, first_column) to indicate it's the oldest column being merged.
                for column in merged_value.columns() {
                    if column.is_some() {
                        callback(None, column.as_ref());
                    }
                }
                // Then for each newer value, we invoke the callback for each column pair (older, newer) before merging.
                for newer_value in values_iter {
                    let newer_value = newer_value.into_decoded(num_columns)?;
                    merged_value = merged_value.merge_with_callback(
                        newer_value,
                        &self.schema,
                        column_family_id,
                        Some(self.ttl_provider.time_provider()),
                        callback,
                    )?;
                }
                self.current_value = Some(KvValue::Decoded(merged_value));
            } else {
                // Decode directly into the batch merge so the value chain is held only once.
                let merged_value = Value::try_merge_all_in_column_family(
                    values
                        .into_iter()
                        .rev()
                        .map(|value| value.into_decoded(num_columns)),
                    &self.schema,
                    column_family_id,
                    Some(self.ttl_provider.time_provider()),
                )?;
                self.current_value = Some(KvValue::Decoded(merged_value));
            }

            // Store the merged value as Decoded to avoid re-encoding
            self.current_key = Some(current_key);

            return Ok(());
        }
    }
}

impl<'a, I> KvIterator<'a> for DeduplicatingIterator<I>
where
    I: KvIterator<'a>,
{
    fn seek(&mut self, target: &[u8]) -> Result<()> {
        self.boundary_state = BoundaryState::None;
        self.inner.seek(target)?;
        self.collect_and_merge()
    }

    fn seek_to_first(&mut self) -> Result<()> {
        self.boundary_state = BoundaryState::None;
        self.inner.seek_to_first()?;
        self.collect_and_merge()
    }

    fn next(&mut self) -> Result<bool> {
        match self.boundary_state {
            BoundaryState::Stopped => return Ok(false),
            BoundaryState::PendingStop => {
                self.boundary_state = BoundaryState::Stopped;
                self.current_key = None;
                self.current_value = None;
                return Ok(false);
            }
            BoundaryState::ReadyToResume => {
                self.boundary_state = BoundaryState::None;
                if !self.inner.next()? {
                    self.current_key = None;
                    self.current_value = None;
                    if self.inner.stopped_at_block_boundary() {
                        self.boundary_state = BoundaryState::Stopped;
                    }
                    return Ok(false);
                }
            }
            BoundaryState::None => {}
        }
        // The inner iterator is already positioned at the next different key
        // (or invalid if no more entries)
        if !self.inner.valid() {
            self.current_key = None;
            self.current_value = None;
            if self.inner.stopped_at_block_boundary() {
                self.boundary_state = BoundaryState::Stopped;
            }
            return Ok(false);
        }

        self.collect_and_merge()?;
        Ok(self.current_key.is_some())
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
        self.boundary_state = BoundaryState::None;
        self.inner.set_stop_at_block_boundary(enabled);
    }

    fn clear_stop_at_block_boundary(&mut self) {
        if self.boundary_state == BoundaryState::Stopped {
            self.boundary_state = BoundaryState::ReadyToResume;
        }
        self.inner.clear_stop_at_block_boundary();
    }

    fn stopped_at_block_boundary(&self) -> bool {
        self.boundary_state == BoundaryState::Stopped
    }
}

#[cfg(test)]
#[path = "../../tests/unit/iterator/deduplicating.rs"]
mod tests;
