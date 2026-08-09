use super::*;
use crate::iterator::mock_iterator::MockIterator;
use crate::merge_operator::MergeOperator;
use crate::sst::row_codec::encode_value;
use crate::r#type::{Column, Value, ValueType};
use std::sync::atomic::{AtomicUsize, Ordering};

struct CountingMergeOperator {
    merge_calls: Arc<AtomicUsize>,
    merge_batch_calls: Arc<AtomicUsize>,
}

impl MergeOperator for CountingMergeOperator {
    fn merge(
        &self,
        existing_value: Bytes,
        value: Bytes,
        _time_provider: Option<&dyn crate::TimeProvider>,
    ) -> Result<(Bytes, Option<ValueType>)> {
        self.merge_calls.fetch_add(1, Ordering::Relaxed);
        let mut merged = existing_value.to_vec();
        merged.extend_from_slice(&value);
        Ok((merged.into(), None))
    }

    fn merge_batch(
        &self,
        existing_value: Bytes,
        operands: Vec<Bytes>,
        _time_provider: Option<&dyn crate::TimeProvider>,
    ) -> Result<(Bytes, Option<ValueType>)> {
        self.merge_batch_calls.fetch_add(1, Ordering::Relaxed);
        let mut merged = existing_value.to_vec();
        for operand in operands {
            merged.extend_from_slice(&operand);
        }
        Ok((merged.into(), None))
    }
}

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
        Some(num_columns),
        Arc::new(TTLProvider::disabled()),
        None,
        Schema::empty(),
    );
    dedup.seek_to_first().unwrap();

    let mut results = vec![];
    while dedup.valid() {
        let (k, kv) = dedup.take_current().unwrap().unwrap();
        let decoded = kv.into_decoded(num_columns).unwrap();
        results.push((k, decoded));
        dedup.next().unwrap();
    }

    assert_eq!(results.len(), 3);
    assert_eq!(results[0].0.as_ref(), b"a");
    assert_eq!(results[1].0.as_ref(), b"b");
    assert_eq!(results[2].0.as_ref(), b"c");
}

#[test]
fn test_deduplicating_materializes_single_merge_by_default() {
    let encoded = make_value_bytes(
        vec![Some(Column::new(ValueType::Merge, b"value".as_slice()))],
        1,
    );
    let mut dedup = DeduplicatingIterator::new(
        MockIterator::new(vec![(b"key".as_slice(), encoded)]),
        Some(1),
        Arc::new(TTLProvider::disabled()),
        None,
        Schema::empty(),
    );

    dedup.seek_to_first().unwrap();
    let (key, value) = dedup.take_current().unwrap().unwrap();
    assert_eq!(key.as_ref(), b"key");
    assert!(!value.is_encoded());
    assert_eq!(
        value.into_decoded(1).unwrap().columns()[0]
            .as_ref()
            .unwrap()
            .data()
            .as_ref(),
        b"value"
    );
}

#[test]
fn test_deduplicating_sst_build_preserves_single_encoded_value() {
    let encoded = make_value_bytes(
        vec![Some(Column::new(ValueType::Merge, b"value".as_slice()))],
        1,
    );
    let mut dedup = DeduplicatingIterator::new_for_sst_build(
        MockIterator::new(vec![(b"key".as_slice(), encoded.as_slice())]),
        Some(1),
        Arc::new(TTLProvider::disabled()),
        None,
        None,
        Schema::empty(),
    );

    dedup.seek_to_first().unwrap();
    let (key, value) = dedup.take_current().unwrap().unwrap();
    assert_eq!(key.as_ref(), b"key");
    assert!(value.is_encoded());
    assert_eq!(value.unwrap_encoded().as_ref(), encoded.as_slice());
}

#[test]
fn test_deduplicating_single_value_still_invokes_callback() {
    let encoded = make_value_bytes(
        vec![Some(Column::new(
            ValueType::PutSeparated,
            b"value".as_slice(),
        ))],
        1,
    );
    let observed = Arc::new(AtomicUsize::new(0));
    let observed_for_callback = Arc::clone(&observed);
    let mut dedup = DeduplicatingIterator::new_for_sst_build(
        MockIterator::new(vec![(b"key".as_slice(), encoded)]),
        Some(1),
        Arc::new(TTLProvider::disabled()),
        Some(Box::new(move |old_column, new_column| {
            assert!(old_column.is_none());
            assert!(new_column.is_some());
            observed_for_callback.fetch_add(1, Ordering::Relaxed);
        })),
        None,
        Schema::empty(),
    );

    dedup.seek_to_first().unwrap();
    let (_, value) = dedup.take_current().unwrap().unwrap();
    assert!(!value.is_encoded());
    assert_eq!(observed.load(Ordering::Relaxed), 1);
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
        Some(num_columns),
        Arc::new(TTLProvider::disabled()),
        None,
        Schema::empty(),
    );
    dedup.seek_to_first().unwrap();

    let mut results = vec![];
    while dedup.valid() {
        let (k, kv) = dedup.take_current().unwrap().unwrap();
        let decoded = kv.into_decoded(num_columns).unwrap();
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
        Some(num_columns),
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
        Some(num_columns),
        Arc::new(TTLProvider::disabled()),
        None,
        Schema::empty(),
    );
    dedup.seek_to_first().unwrap();

    let (k, kv) = dedup.take_current().unwrap().unwrap();
    let decoded = kv.into_decoded(num_columns).unwrap();

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
        Some(num_columns),
        Arc::new(TTLProvider::disabled()),
        None,
        Schema::empty(),
    );
    dedup.seek_to_first().unwrap();

    let (k, kv) = dedup.take_current().unwrap().unwrap();
    let decoded = kv.into_decoded(num_columns).unwrap();

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
fn test_deduplicating_uses_one_batch_merge_without_callback() {
    let merge_calls = Arc::new(AtomicUsize::new(0));
    let merge_batch_calls = Arc::new(AtomicUsize::new(0));
    let schema = Schema::new(
        0,
        1,
        vec![Arc::new(CountingMergeOperator {
            merge_calls: Arc::clone(&merge_calls),
            merge_batch_calls: Arc::clone(&merge_batch_calls),
        })],
    );
    let entries = vec![
        (
            b"a".as_slice(),
            make_value_bytes(
                vec![Some(Column::new(ValueType::Merge, b"-3".as_slice()))],
                1,
            ),
        ),
        (
            b"a".as_slice(),
            make_value_bytes(
                vec![Some(Column::new(ValueType::Merge, b"-2".as_slice()))],
                1,
            ),
        ),
        (
            b"a".as_slice(),
            make_value_bytes(
                vec![Some(Column::new(ValueType::Merge, b"-1".as_slice()))],
                1,
            ),
        ),
        (
            b"a".as_slice(),
            make_value_bytes(
                vec![Some(Column::new(ValueType::Put, b"base".as_slice()))],
                1,
            ),
        ),
    ];
    let mut dedup = DeduplicatingIterator::new(
        MockIterator::new(entries),
        Some(1),
        Arc::new(TTLProvider::disabled()),
        None,
        Arc::new(schema),
    );

    dedup.seek_to_first().unwrap();
    let (key, value) = dedup.take_current().unwrap().unwrap();
    let value = value.into_decoded(1).unwrap();
    assert_eq!(key.as_ref(), b"a");
    assert_eq!(
        value.columns()[0].as_ref().unwrap().data().as_ref(),
        b"base-1-2-3"
    );
    assert_eq!(merge_batch_calls.load(Ordering::Relaxed), 1);
    assert_eq!(merge_calls.load(Ordering::Relaxed), 0);
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
        Some(num_columns),
        Arc::new(TTLProvider::disabled()),
        None,
        Schema::empty(),
    );
    dedup.seek_to_first().unwrap();

    let (k, kv) = dedup.take_current().unwrap().unwrap();
    let decoded = kv.into_decoded(num_columns).unwrap();

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
        Some(1),
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
        Some(num_columns),
        Arc::new(TTLProvider::disabled()),
        None,
        Schema::empty(),
    );

    dedup.seek(b"b").unwrap();
    assert!(dedup.valid());
    assert_eq!(dedup.key().unwrap().unwrap(), b"b");
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
        Some(num_columns),
        Arc::new(TTLProvider::disabled()),
        None,
        Schema::empty(),
    );
    dedup.seek_to_first().unwrap();

    let (k, kv) = dedup.take_current().unwrap().unwrap();
    let decoded = kv.into_decoded(num_columns).unwrap();
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
        Some(num_columns),
        ttl_provider.clone(),
        None,
        Schema::empty(),
    );
    dedup.seek_to_first().unwrap();

    let mut results = vec![];
    while dedup.valid() {
        let (k, kv) = dedup.take_current().unwrap().unwrap();
        let decoded = kv.into_decoded(num_columns).unwrap();
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
        Some(num_columns),
        Arc::new(TTLProvider::disabled()),
        None,
        Schema::empty(),
    );
    dedup.seek_to_first().unwrap();

    let mut results = vec![];
    while dedup.valid() {
        let (k, kv) = dedup.take_current().unwrap().unwrap();
        let decoded = kv.into_decoded(num_columns).unwrap();
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
