//! Deduplicating iterator that merges values with the same key.
//!
//! This module provides a wrapper iterator that merges duplicate keys
//! by combining their values according to the merge semantics defined
//! in the `Value` type.

use crate::error::Result;
use crate::iterator::KvIterator;
use crate::schema::Schema;
use crate::sst::row_codec::{decode_value, encode_value, value_expired_at, value_is_terminal};
use crate::ttl::TTLProvider;
use crate::r#type::Column;
use bytes::Bytes;
use std::sync::Arc;

/// Callback type for column merges. The callback is invoked for every pair of merged columns,
/// with the older column (if any) and the newer column (if any). And also the oldest first column
/// is guaranteed to be called first as well, as callback(None, oldest_column) before any
/// newer column is merged.
type MergeCallback = Box<dyn FnMut(Option<&Column>, Option<&Column>)>;

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
    /// Number of columns in the value schema.
    num_columns: usize,
    /// Current merged key (if valid).
    current_key: Option<Bytes>,
    /// Current merged value (if valid).
    current_value: Option<Bytes>,
    /// TTL provider to evaluate expiration.
    ttl_provider: Arc<TTLProvider>,
    /// Callback invoked for every merged column pair (older, newer).
    on_merge: Option<MergeCallback>,
    /// Whether to allow terminal fast-path that skips collecting older versions.
    allow_terminal_shortcut: bool,
    /// Merge operators used for column merge semantics.
    schema: Arc<Schema>,
}

/// Collects a value slice into the values vector or selects it as the final value.
/// This function checks for expiration and terminal status.
fn collect_value(
    value_slice: &[u8],
    num_columns: usize,
    ttl_provider: &TTLProvider,
    allow_terminal_shortcut: bool,
    values: &mut Vec<Bytes>,
    selected_value: &mut Option<Bytes>,
    stop_collecting: &mut bool,
) -> Result<()> {
    let expired_at = value_expired_at(value_slice)?;
    if ttl_provider.expired(&expired_at) {
        return Ok(());
    }
    let is_terminal = value_is_terminal(value_slice, num_columns)?;
    if allow_terminal_shortcut && selected_value.is_none() && values.is_empty() && is_terminal {
        *selected_value = Some(Bytes::copy_from_slice(value_slice));
        *stop_collecting = true;
        return Ok(());
    }
    values.push(Bytes::copy_from_slice(value_slice));
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
    /// * `num_columns` - Number of columns in the value schema.
    pub fn new(
        inner: I,
        num_columns: usize,
        ttl_provider: Arc<TTLProvider>,
        on_merge: Option<MergeCallback>,
        schema: Arc<Schema>,
    ) -> Self {
        let allow_terminal_shortcut = on_merge.is_none();
        Self {
            inner,
            num_columns,
            current_key: None,
            current_value: None,
            ttl_provider,
            on_merge,
            allow_terminal_shortcut,
            schema,
        }
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
                return Ok(());
            }

            // Get the first key-value pair
            let current = self.inner.current_slice()?;
            let Some((key_slice, value_slice)) = current else {
                self.current_key = None;
                self.current_value = None;
                return Ok(());
            };

            let current_key = Bytes::copy_from_slice(key_slice);

            let mut values: Vec<Bytes> = Vec::new();
            let mut selected_value: Option<Bytes> = None;
            let mut stop_collecting = false;

            collect_value(
                value_slice,
                self.num_columns,
                &self.ttl_provider,
                allow_terminal_shortcut,
                &mut values,
                &mut selected_value,
                &mut stop_collecting,
            )?;

            // Advance to next entry and check for same key
            while self.inner.next()? {
                let next_key = self.inner.key_slice()?;
                let Some(next_key) = next_key else {
                    break;
                };
                if next_key != current_key.as_ref() {
                    // Different key, stop collecting
                    break;
                }
                if stop_collecting && allow_terminal_shortcut {
                    continue;
                }

                // Same key, collect the value
                if let Some(next_value_bytes) = self.inner.value_slice()? {
                    collect_value(
                        next_value_bytes,
                        self.num_columns,
                        &self.ttl_provider,
                        allow_terminal_shortcut,
                        &mut values,
                        &mut selected_value,
                        &mut stop_collecting,
                    )?;
                }
            }

            if let Some(value) = selected_value {
                self.current_key = Some(current_key);
                self.current_value = Some(value);
                return Ok(());
            }

            if values.is_empty() {
                // All versions for this key are expired; continue to the next key.
                continue;
            }

            // Merge from oldest to newest (reverse order)
            // The last value in the list is the oldest, the first is the newest
            let mut values_iter = values.into_iter().rev();
            let first = values_iter.next().expect("values is non-empty");
            let mut first = first;
            let mut merged_value = decode_value(&mut first, self.num_columns)?;

            if let Some(callback) = self.on_merge.as_deref_mut() {
                // The first column is invoked with callback(None, first_column) to indicate it's the oldest column being merged.
                for column in merged_value.columns() {
                    if column.is_some() {
                        callback(None, column.as_ref());
                    }
                }
                // Then for each newer value, we invoke the callback for each column pair (older, newer) before merging.
                for newer_value in values_iter {
                    let mut newer_value = newer_value;
                    let newer_value = decode_value(&mut newer_value, self.num_columns)?;
                    merged_value = merged_value.merge_with_callback(
                        newer_value,
                        &self.schema,
                        Some(self.ttl_provider.time_provider()),
                        callback,
                    )?;
                }
            } else {
                for newer_value in values_iter {
                    let mut newer_value = newer_value;
                    let newer_value = decode_value(&mut newer_value, self.num_columns)?;
                    merged_value = merged_value.merge(
                        newer_value,
                        &self.schema,
                        Some(self.ttl_provider.time_provider()),
                    )?;
                }
            }

            // Encode the merged value
            self.current_key = Some(current_key);
            self.current_value = Some(encode_value(&merged_value, self.num_columns));

            return Ok(());
        }
    }
}

impl<'a, I> KvIterator<'a> for DeduplicatingIterator<I>
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
        // The inner iterator is already positioned at the next different key
        // (or invalid if no more entries)
        if !self.inner.valid() {
            self.current_key = None;
            self.current_value = None;
            return Ok(false);
        }

        self.collect_and_merge()?;
        Ok(self.current_key.is_some())
    }

    fn valid(&self) -> bool {
        self.current_key.is_some()
    }

    fn key(&self) -> Result<Option<Bytes>> {
        Ok(self.current_key.clone())
    }

    fn key_slice(&self) -> Result<Option<&[u8]>> {
        Ok(self.current_key.as_deref())
    }

    fn value(&self) -> Result<Option<Bytes>> {
        Ok(self.current_value.clone())
    }

    fn value_slice(&self) -> Result<Option<&[u8]>> {
        Ok(self.current_value.as_deref())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::iterator::mock_iterator::MockIterator;
    use crate::sst::row_codec::encode_value;
    use crate::r#type::{Column, Value, ValueType};

    fn make_value_bytes(columns: Vec<Option<Column>>, num_columns: usize) -> Vec<u8> {
        let value = Value::new(columns);
        encode_value(&value, num_columns).to_vec()
    }

    fn make_value_bytes_with_expiry(
        columns: Vec<Option<Column>>,
        num_columns: usize,
        expired_at: Option<u32>,
    ) -> Vec<u8> {
        let value = Value::new_with_expired_at(columns, expired_at);
        encode_value(&value, num_columns).to_vec()
    }

    #[test]
    fn test_deduplicating_no_duplicates() {
        let num_columns = 1;

        let entries: Vec<(&[u8], Vec<u8>)> = vec![
            (
                b"a",
                make_value_bytes(
                    vec![Some(Column::new(ValueType::Put, b"v1".to_vec()))],
                    num_columns,
                ),
            ),
            (
                b"b",
                make_value_bytes(
                    vec![Some(Column::new(ValueType::Put, b"v2".to_vec()))],
                    num_columns,
                ),
            ),
            (
                b"c",
                make_value_bytes(
                    vec![Some(Column::new(ValueType::Put, b"v3".to_vec()))],
                    num_columns,
                ),
            ),
        ];

        let iter = MockIterator::new(entries);
        let mut dedup = DeduplicatingIterator::new(
            iter,
            num_columns,
            Arc::new(TTLProvider::disabled()),
            None,
            Schema::empty(),
        );
        dedup.seek_to_first().unwrap();

        let mut results = vec![];
        while dedup.valid() {
            let (k, mut v) = dedup.current().unwrap().unwrap();
            let decoded = decode_value(&mut v, num_columns).unwrap();
            results.push((k, decoded));
            dedup.next().unwrap();
        }

        assert_eq!(results.len(), 3);
        assert_eq!(results[0].0.as_ref(), b"a");
        assert_eq!(results[1].0.as_ref(), b"b");
        assert_eq!(results[2].0.as_ref(), b"c");
    }

    #[test]
    fn test_deduplicating_with_put_overwrites() {
        let num_columns = 1;

        // Same key "a" appears twice - newer put should win
        let entries: Vec<(&[u8], Vec<u8>)> = vec![
            (
                b"a",
                make_value_bytes(
                    vec![Some(Column::new(ValueType::Put, b"new".to_vec()))],
                    num_columns,
                ),
            ),
            (
                b"a",
                make_value_bytes(
                    vec![Some(Column::new(ValueType::Put, b"old".to_vec()))],
                    num_columns,
                ),
            ),
            (
                b"b",
                make_value_bytes(
                    vec![Some(Column::new(ValueType::Put, b"v2".to_vec()))],
                    num_columns,
                ),
            ),
        ];

        let iter = MockIterator::new(entries);
        let mut dedup = DeduplicatingIterator::new(
            iter,
            num_columns,
            Arc::new(TTLProvider::disabled()),
            None,
            Schema::empty(),
        );
        dedup.seek_to_first().unwrap();

        let mut results = vec![];
        while dedup.valid() {
            let (k, mut v) = dedup.current().unwrap().unwrap();
            let decoded = decode_value(&mut v, num_columns).unwrap();
            results.push((k, decoded));
            dedup.next().unwrap();
        }

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0.as_ref(), b"a");
        // The newer "new" value merged with older "old" - since newer is Put, it replaces
        assert_eq!(
            results[0].1.columns()[0].as_ref().unwrap().data().as_ref(),
            b"new"
        );
        assert_eq!(results[1].0.as_ref(), b"b");
    }

    #[test]
    fn test_deduplicating_merge_callback() {
        let num_columns = 1;
        let entries: Vec<(&[u8], Vec<u8>)> = vec![
            (
                b"a",
                make_value_bytes(
                    vec![Some(Column::new(ValueType::Put, b"new".to_vec()))],
                    num_columns,
                ),
            ),
            (
                b"a",
                make_value_bytes(
                    vec![Some(Column::new(ValueType::PutSeparated, b"old".to_vec()))],
                    num_columns,
                ),
            ),
        ];
        let iter = MockIterator::new(entries);
        let overlapped = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
        let overlapped_for_callback = std::rc::Rc::clone(&overlapped);
        let mut dedup = DeduplicatingIterator::new(
            iter,
            num_columns,
            Arc::new(TTLProvider::disabled()),
            Some(Box::new(move |old_column, _new_column| {
                if let Some(old_column) = old_column {
                    overlapped_for_callback
                        .borrow_mut()
                        .push(old_column.value_type);
                }
            })),
            Schema::empty(),
        );
        dedup.seek_to_first().unwrap();
        assert!(dedup.valid());
        assert_eq!(&*overlapped.borrow(), &[ValueType::PutSeparated]);
    }

    #[test]
    fn test_deduplicating_with_merge_concatenates() {
        let num_columns = 1;

        // Same key "a" appears twice - newer merge should concatenate
        let entries: Vec<(&[u8], Vec<u8>)> = vec![
            (
                b"a",
                make_value_bytes(
                    vec![Some(Column::new(ValueType::Merge, b"_suffix".to_vec()))],
                    num_columns,
                ),
            ),
            (
                b"a",
                make_value_bytes(
                    vec![Some(Column::new(ValueType::Put, b"base".to_vec()))],
                    num_columns,
                ),
            ),
        ];

        let iter = MockIterator::new(entries);
        let mut dedup = DeduplicatingIterator::new(
            iter,
            num_columns,
            Arc::new(TTLProvider::disabled()),
            None,
            Schema::empty(),
        );
        dedup.seek_to_first().unwrap();

        let (k, mut v) = dedup.current().unwrap().unwrap();
        let decoded = decode_value(&mut v, num_columns).unwrap();

        assert_eq!(k.as_ref(), b"a");
        // The merge operation concatenates: "base" + "_suffix" should be how it works
        // But wait - the order matters. The first entry is "newer" (Merge with "_suffix")
        // and second is "older" (Put with "base").
        // The merge operation: older.merge(newer) = "base".merge("_suffix") where newer is Merge
        // So result should be "base_suffix"
        assert_eq!(
            decoded.columns()[0].as_ref().unwrap().data().as_ref(),
            b"base_suffix"
        );
    }

    #[test]
    fn test_deduplicating_multiple_same_keys() {
        let num_columns = 1;

        // Three entries with same key
        // Order in iterator: newest first, oldest last
        // Entry 1 (newest): Merge "3"
        // Entry 2: Merge "2"
        // Entry 3 (oldest): Put "1"
        let entries: Vec<(&[u8], Vec<u8>)> = vec![
            (
                b"a",
                make_value_bytes(
                    vec![Some(Column::new(ValueType::Merge, b"3".to_vec()))],
                    num_columns,
                ),
            ),
            (
                b"a",
                make_value_bytes(
                    vec![Some(Column::new(ValueType::Merge, b"2".to_vec()))],
                    num_columns,
                ),
            ),
            (
                b"a",
                make_value_bytes(
                    vec![Some(Column::new(ValueType::Put, b"1".to_vec()))],
                    num_columns,
                ),
            ),
        ];

        let iter = MockIterator::new(entries);
        let mut dedup = DeduplicatingIterator::new(
            iter,
            num_columns,
            Arc::new(TTLProvider::disabled()),
            None,
            Schema::empty(),
        );
        dedup.seek_to_first().unwrap();

        let (k, mut v) = dedup.current().unwrap().unwrap();
        let decoded = decode_value(&mut v, num_columns).unwrap();

        assert_eq!(k.as_ref(), b"a");
        // Merge order: oldest to newest
        // 1. Start with oldest: Put "1"
        // 2. Merge with Merge "2": "1".merge("2") = "12" (concatenate)
        // 3. Merge with Merge "3": "12".merge("3") = "123" (concatenate)
        assert_eq!(
            decoded.columns()[0].as_ref().unwrap().data().as_ref(),
            b"123"
        );
    }

    #[test]
    fn test_deduplicating_with_delete() {
        let num_columns = 1;

        let entries: Vec<(&[u8], Vec<u8>)> = vec![
            (
                b"a",
                make_value_bytes(
                    vec![Some(Column::new(ValueType::Delete, b"".to_vec()))],
                    num_columns,
                ),
            ),
            (
                b"a",
                make_value_bytes(
                    vec![Some(Column::new(ValueType::Put, b"old".to_vec()))],
                    num_columns,
                ),
            ),
        ];

        let iter = MockIterator::new(entries);
        let mut dedup = DeduplicatingIterator::new(
            iter,
            num_columns,
            Arc::new(TTLProvider::disabled()),
            None,
            Schema::empty(),
        );
        dedup.seek_to_first().unwrap();

        let (k, mut v) = dedup.current().unwrap().unwrap();
        let decoded = decode_value(&mut v, num_columns).unwrap();

        assert_eq!(k.as_ref(), b"a");
        // Delete replaces the old value
        assert!(matches!(
            decoded.columns()[0].as_ref().unwrap().value_type(),
            ValueType::Delete
        ));
    }

    #[test]
    fn test_deduplicating_empty() {
        let iter = MockIterator::new(Vec::<(&[u8], &[u8])>::new());
        let mut dedup = DeduplicatingIterator::new(
            iter,
            1,
            Arc::new(TTLProvider::disabled()),
            None,
            Schema::empty(),
        );
        dedup.seek_to_first().unwrap();

        assert!(!dedup.valid());
    }

    #[test]
    fn test_deduplicating_seek() {
        let num_columns = 1;

        let entries: Vec<(&[u8], Vec<u8>)> = vec![
            (
                b"a",
                make_value_bytes(
                    vec![Some(Column::new(ValueType::Put, b"v1".to_vec()))],
                    num_columns,
                ),
            ),
            (
                b"b",
                make_value_bytes(
                    vec![Some(Column::new(ValueType::Put, b"v2".to_vec()))],
                    num_columns,
                ),
            ),
            (
                b"c",
                make_value_bytes(
                    vec![Some(Column::new(ValueType::Put, b"v3".to_vec()))],
                    num_columns,
                ),
            ),
        ];

        let iter = MockIterator::new(entries);
        let mut dedup = DeduplicatingIterator::new(
            iter,
            num_columns,
            Arc::new(TTLProvider::disabled()),
            None,
            Schema::empty(),
        );

        dedup.seek(b"b").unwrap();
        assert!(dedup.valid());
        assert_eq!(dedup.key().unwrap().unwrap().as_ref(), b"b");
    }

    #[test]
    fn test_deduplicating_multi_column() {
        let num_columns = 2;

        // First entry: both columns present
        let v1 = make_value_bytes(
            vec![
                Some(Column::new(ValueType::Put, b"col1_new".to_vec())),
                Some(Column::new(ValueType::Merge, b"_append".to_vec())),
            ],
            num_columns,
        );

        // Second entry: older value
        let v2 = make_value_bytes(
            vec![
                Some(Column::new(ValueType::Put, b"col1_old".to_vec())),
                Some(Column::new(ValueType::Put, b"col2_old".to_vec())),
            ],
            num_columns,
        );

        let entries: Vec<(&[u8], Vec<u8>)> = vec![(b"a", v1), (b"a", v2)];

        let iter = MockIterator::new(entries);
        let mut dedup = DeduplicatingIterator::new(
            iter,
            num_columns,
            Arc::new(TTLProvider::disabled()),
            None,
            Schema::empty(),
        );
        dedup.seek_to_first().unwrap();

        let (k, mut v) = dedup.current().unwrap().unwrap();
        let decoded = decode_value(&mut v, num_columns).unwrap();
        let cols = decoded.columns();

        assert_eq!(k.as_ref(), b"a");
        // Column 0: Put replaces -> "col1_new"
        assert_eq!(cols[0].as_ref().unwrap().data().as_ref(), b"col1_new");
        // Column 1: Merge appends -> "col2_old_append"
        assert_eq!(
            cols[1].as_ref().unwrap().data().as_ref(),
            b"col2_old_append"
        );
    }

    #[test]
    fn test_deduplicating_skips_expired_for_key() {
        let num_columns = 1;
        let ttl_provider = Arc::new(TTLProvider::new(
            &crate::ttl::TtlConfig {
                enabled: true,
                default_ttl_seconds: None,
            },
            Arc::new(crate::time::ManualTimeProvider::new(10)),
        ));
        let now = ttl_provider.now_seconds();

        // Key "a": newest is expired, older is valid -> should return older
        // Key "b": both expired -> should be skipped entirely
        // Key "c": valid
        let entries: Vec<(&[u8], Vec<u8>)> = vec![
            (
                b"a",
                make_value_bytes_with_expiry(
                    vec![Some(Column::new(ValueType::Put, b"new".to_vec()))],
                    num_columns,
                    Some(now - 1),
                ),
            ),
            (
                b"a",
                make_value_bytes_with_expiry(
                    vec![Some(Column::new(ValueType::Put, b"old".to_vec()))],
                    num_columns,
                    None,
                ),
            ),
            (
                b"b",
                make_value_bytes_with_expiry(
                    vec![Some(Column::new(ValueType::Put, b"b_new".to_vec()))],
                    num_columns,
                    Some(now - 1),
                ),
            ),
            (
                b"b",
                make_value_bytes_with_expiry(
                    vec![Some(Column::new(ValueType::Put, b"b_old".to_vec()))],
                    num_columns,
                    Some(now - 1),
                ),
            ),
            (
                b"c",
                make_value_bytes_with_expiry(
                    vec![Some(Column::new(ValueType::Put, b"c".to_vec()))],
                    num_columns,
                    None,
                ),
            ),
        ];

        let iter = MockIterator::new(entries);
        let mut dedup = DeduplicatingIterator::new(
            iter,
            num_columns,
            ttl_provider.clone(),
            None,
            Schema::empty(),
        );
        dedup.seek_to_first().unwrap();

        let mut results = vec![];
        while dedup.valid() {
            let (k, mut v) = dedup.current().unwrap().unwrap();
            let decoded = decode_value(&mut v, num_columns).unwrap();
            results.push((k, decoded));
            dedup.next().unwrap();
        }

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0.as_ref(), b"a");
        assert_eq!(
            results[0].1.columns()[0].as_ref().unwrap().data().as_ref(),
            b"old"
        );
        assert_eq!(results[1].0.as_ref(), b"c");
        assert_eq!(
            results[1].1.columns()[0].as_ref().unwrap().data().as_ref(),
            b"c"
        );
    }

    #[test]
    fn test_deduplicating_with_merging_iterator() {
        use crate::iterator::MergingIterator;

        let num_columns = 1;

        // Simulate two SortedRuns with overlapping keys
        // SortedRun 1 (newer): has key "a" with Merge "suffix", key "b" with Put "b1"
        // SortedRun 2 (older): has key "a" with Put "base", key "c" with Put "c1"
        let iter1 = MockIterator::new(vec![
            (
                b"a" as &[u8],
                make_value_bytes(
                    vec![Some(Column::new(ValueType::Merge, b"_suffix".to_vec()))],
                    num_columns,
                ),
            ),
            (
                b"b" as &[u8],
                make_value_bytes(
                    vec![Some(Column::new(ValueType::Put, b"b1".to_vec()))],
                    num_columns,
                ),
            ),
        ]);

        let iter2 = MockIterator::new(vec![
            (
                b"a" as &[u8],
                make_value_bytes(
                    vec![Some(Column::new(ValueType::Put, b"base".to_vec()))],
                    num_columns,
                ),
            ),
            (
                b"c" as &[u8],
                make_value_bytes(
                    vec![Some(Column::new(ValueType::Put, b"c1".to_vec()))],
                    num_columns,
                ),
            ),
        ]);

        // Create MergingIterator from both runs
        let merging_iter = MergingIterator::new(vec![iter1, iter2]);

        // Wrap with DeduplicatingIterator
        let mut dedup = DeduplicatingIterator::new(
            merging_iter,
            num_columns,
            Arc::new(TTLProvider::disabled()),
            None,
            Schema::empty(),
        );
        dedup.seek_to_first().unwrap();

        let mut results = vec![];
        while dedup.valid() {
            let (k, mut v) = dedup.current().unwrap().unwrap();
            let decoded = decode_value(&mut v, num_columns).unwrap();
            results.push((k, decoded));
            dedup.next().unwrap();
        }

        // Should have 3 unique keys: a, b, c
        assert_eq!(results.len(), 3);

        // Key "a" should be merged: "base" + "_suffix" = "base_suffix"
        assert_eq!(results[0].0.as_ref(), b"a");
        assert_eq!(
            results[0].1.columns()[0].as_ref().unwrap().data().as_ref(),
            b"base_suffix"
        );

        // Key "b" should be unchanged
        assert_eq!(results[1].0.as_ref(), b"b");
        assert_eq!(
            results[1].1.columns()[0].as_ref().unwrap().data().as_ref(),
            b"b1"
        );

        // Key "c" should be unchanged
        assert_eq!(results[2].0.as_ref(), b"c");
        assert_eq!(
            results[2].1.columns()[0].as_ref().unwrap().data().as_ref(),
            b"c1"
        );
    }
}
