use super::*;
use crate::merge_operator::{MergeOperator, default_merge_operator_ref};
use crate::sst::row_codec::encode_value;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

struct PanicMergeOperator;

impl MergeOperator for PanicMergeOperator {
    fn merge(
        &self,
        _existing_value: Bytes,
        _value: Bytes,
        _time_provider: Option<&dyn TimeProvider>,
    ) -> Result<(Bytes, Option<ValueType>)> {
        panic!("merge operator should not be invoked");
    }
}

struct TerminalBatchMergeOperator;

impl MergeOperator for TerminalBatchMergeOperator {
    fn merge(
        &self,
        existing_value: Bytes,
        value: Bytes,
        _time_provider: Option<&dyn TimeProvider>,
    ) -> Result<(Bytes, Option<ValueType>)> {
        let mut merged = BytesMut::with_capacity(existing_value.len() + value.len());
        merged.extend_from_slice(existing_value.as_ref());
        merged.extend_from_slice(value.as_ref());
        Ok((merged.freeze(), Some(ValueType::Put)))
    }

    fn merge_batch(
        &self,
        existing_value: Bytes,
        operands: Vec<Bytes>,
        _time_provider: Option<&dyn TimeProvider>,
    ) -> Result<(Bytes, Option<ValueType>)> {
        let mut merged = BytesMut::with_capacity(
            existing_value.len() + operands.iter().map(Bytes::len).sum::<usize>(),
        );
        merged.extend_from_slice(existing_value.as_ref());
        for operand in operands {
            merged.extend_from_slice(operand.as_ref());
        }
        Ok((merged.freeze(), Some(ValueType::Put)))
    }
}

fn merge_values_pairwise(values: Vec<Value>, schema: &Schema) -> Value {
    let mut values = values.into_iter();
    let mut merged = values.next().expect("test values not empty");
    for newer in values {
        merged = merged
            .merge_in_column_family(newer, schema, DEFAULT_COLUMN_FAMILY_ID, None)
            .unwrap();
    }
    merged
}

fn value_signature(value: &Value) -> Vec<Option<(ValueType, Vec<u8>)>> {
    value
        .columns()
        .iter()
        .map(|column| {
            column
                .as_ref()
                .map(|column| (column.value_type, column.data().as_ref().to_vec()))
        })
        .collect()
}

fn assert_batch_matches_pairwise(values: Vec<Value>, schema: &Schema) {
    let pairwise = merge_values_pairwise(values.clone(), schema);
    let batched =
        Value::merge_all_in_column_family(values, schema, DEFAULT_COLUMN_FAMILY_ID, None).unwrap();
    assert_eq!(value_signature(&batched), value_signature(&pairwise));
    assert_eq!(batched.expired_at(), pairwise.expired_at());
}

fn assert_fallible_batch_matches_pairwise(values: Vec<Value>, schema: &Schema) {
    let pairwise = merge_values_pairwise(values.clone(), schema);
    let batched = Value::try_merge_all_in_column_family(
        values.into_iter().map(Ok),
        schema,
        DEFAULT_COLUMN_FAMILY_ID,
        None,
    )
    .unwrap();
    assert_eq!(value_signature(&batched), value_signature(&pairwise));
    assert_eq!(batched.expired_at(), pairwise.expired_at());
}

fn merge_single_column(values: Vec<Value>, schema: &Schema) -> Column {
    Value::merge_all_in_column_family(values, schema, DEFAULT_COLUMN_FAMILY_ID, None)
        .unwrap()
        .columns
        .into_iter()
        .next()
        .flatten()
        .expect("merged column")
}

fn assert_separated_array(
    column: &Column,
    expected_type: ValueType,
    expected_items: &[(ValueType, &[u8])],
) {
    assert_eq!(column.value_type, expected_type);
    let items = decode_merge_separated_array(column.data()).unwrap();
    assert_eq!(items.len(), expected_items.len());
    for (item, expected) in items.iter().zip(expected_items) {
        assert_eq!((item.value_type, item.data()), *expected);
    }
}

#[test]
fn test_value_merge_all_matches_put_and_many_merges() {
    let values = vec![
        Value::new(vec![Some(Column::new(ValueType::Put, b"base".as_slice()))]),
        Value::new(vec![Some(Column::new(ValueType::Merge, b"-a".as_slice()))]),
        Value::new(vec![Some(Column::new(ValueType::Merge, b"-b".as_slice()))]),
        Value::new(vec![Some(Column::new(ValueType::Merge, b"-c".as_slice()))]),
    ];
    let schema = Schema::empty();
    assert_batch_matches_pairwise(values.clone(), schema.as_ref());
    assert_fallible_batch_matches_pairwise(values, schema.as_ref());
}

#[test]
fn test_value_merge_all_matches_delete_and_merge_only_histories() {
    let schema = Schema::empty();
    let delete_history = vec![
        Value::new(vec![Some(Column::new(ValueType::Delete, Bytes::new()))]),
        Value::new(vec![Some(Column::new(ValueType::Merge, b"a".as_slice()))]),
        Value::new(vec![Some(Column::new(ValueType::Merge, b"b".as_slice()))]),
    ];
    assert_batch_matches_pairwise(delete_history, schema.as_ref());

    let merge_only_history = vec![
        Value::new(vec![Some(Column::new(ValueType::Merge, b"a".as_slice()))]),
        Value::new(vec![Some(Column::new(ValueType::Merge, b"b".as_slice()))]),
        Value::new(vec![Some(Column::new(ValueType::Merge, b"c".as_slice()))]),
    ];
    let batched = Value::merge_all_in_column_family(
        merge_only_history.clone(),
        schema.as_ref(),
        DEFAULT_COLUMN_FAMILY_ID,
        None,
    )
    .unwrap();
    assert_batch_matches_pairwise(merge_only_history, schema.as_ref());
    assert_eq!(
        batched.columns()[0].as_ref().unwrap().value_type,
        ValueType::Merge
    );
}

#[test]
fn test_value_merge_all_preserves_custom_terminal_override() {
    let schema = Schema::new(0, 1, vec![Arc::new(TerminalBatchMergeOperator)]);
    let values = vec![
        Value::new(vec![Some(Column::new(
            ValueType::Merge,
            b"base".as_slice(),
        ))]),
        Value::new(vec![Some(Column::new(ValueType::Merge, b"-a".as_slice()))]),
        Value::new(vec![Some(Column::new(ValueType::Merge, b"-b".as_slice()))]),
    ];
    let batched =
        Value::merge_all_in_column_family(values.clone(), &schema, DEFAULT_COLUMN_FAMILY_ID, None)
            .unwrap();
    assert_batch_matches_pairwise(values, &schema);
    assert_eq!(
        batched.columns()[0].as_ref().unwrap().value_type,
        ValueType::Put
    );
}

#[test]
fn test_value_merge_all_matches_sparse_multi_column_history() {
    let values = vec![
        Value::new(vec![
            Some(Column::new(ValueType::Put, b"c0".as_slice())),
            Some(Column::new(ValueType::Put, b"c1".as_slice())),
        ]),
        Value::new(vec![
            Some(Column::new(ValueType::Merge, b"-a".as_slice())),
            None,
        ]),
        Value::new(vec![
            None,
            Some(Column::new(ValueType::Merge, b"-b".as_slice())),
        ]),
        Value::new(vec![
            Some(Column::new(ValueType::Merge, b"-c".as_slice())),
            None,
        ]),
    ];
    let schema = Schema::empty();
    assert_batch_matches_pairwise(values.clone(), schema.as_ref());
    assert_fallible_batch_matches_pairwise(values, schema.as_ref());
}

#[test]
fn test_value_try_merge_all_stops_after_decode_error() {
    struct DecodingValues {
        values: std::vec::IntoIter<KvValue>,
        next_calls: Arc<AtomicUsize>,
    }

    impl Iterator for DecodingValues {
        type Item = Result<Value>;

        fn next(&mut self) -> Option<Self::Item> {
            let value = self.values.next()?;
            self.next_calls.fetch_add(1, Ordering::Relaxed);
            Some(value.into_decoded(1))
        }

        fn size_hint(&self) -> (usize, Option<usize>) {
            self.values.size_hint()
        }
    }

    let encoded = |value_type, data: &'static [u8]| {
        KvValue::Encoded(encode_value(
            &Value::new(vec![Some(Column::new(value_type, data))]),
            1,
        ))
    };
    let next_calls = Arc::new(AtomicUsize::new(0));
    let values = DecodingValues {
        values: vec![
            encoded(ValueType::Put, b"base"),
            KvValue::Encoded(Bytes::from_static(&[0])),
            encoded(ValueType::Merge, b"-unread"),
        ]
        .into_iter(),
        next_calls: Arc::clone(&next_calls),
    };

    let result = Value::try_merge_all_in_column_family(
        values,
        Schema::empty().as_ref(),
        DEFAULT_COLUMN_FAMILY_ID,
        None,
    );
    assert!(result.is_err());
    assert_eq!(next_calls.load(Ordering::Relaxed), 2);
}

#[test]
fn test_value_merge_all_accumulates_inline_and_separated_merges_once() {
    let values = vec![
        Value::new(vec![Some(Column::new(ValueType::Put, b"base".as_slice()))]),
        Value::new(vec![Some(Column::new(
            ValueType::Merge,
            b"-inline".as_slice(),
        ))]),
        Value::new(vec![Some(Column::new(
            ValueType::MergeSeparated,
            b"separated".as_slice(),
        ))]),
        Value::new(vec![Some(Column::new(
            ValueType::Merge,
            b"-tail".as_slice(),
        ))]),
    ];
    let schema = Schema::new(0, 1, vec![Arc::new(PanicMergeOperator)]);
    let column = merge_single_column(values, &schema);
    assert_separated_array(
        &column,
        ValueType::PutSeparatedArray,
        &[
            (ValueType::Put, b"base".as_slice()),
            (ValueType::Merge, b"-inline".as_slice()),
            (ValueType::MergeSeparated, b"separated".as_slice()),
            (ValueType::Merge, b"-tail".as_slice()),
        ],
    );
}

#[test]
fn test_value_merge_all_flattens_nested_separated_arrays() {
    let nested_columns = [
        Column::new(ValueType::MergeSeparated, b"separated-a".as_slice()),
        Column::new(ValueType::Merge, b"-inline".as_slice()),
    ];
    let nested_refs: Vec<_> = nested_columns.iter().map(Column::as_ref_column).collect();
    let nested = encode_merge_separated_array(&nested_refs).unwrap();
    let values = vec![
        Value::new(vec![Some(Column::new(
            ValueType::PutSeparated,
            b"base".as_slice(),
        ))]),
        Value::new(vec![Some(Column::new(
            ValueType::MergeSeparatedArray,
            nested,
        ))]),
        Value::new(vec![Some(Column::new(
            ValueType::MergeSeparated,
            b"separated-b".as_slice(),
        ))]),
    ];

    let column = merge_single_column(values, Schema::empty().as_ref());
    assert_separated_array(
        &column,
        ValueType::PutSeparatedArray,
        &[
            (ValueType::PutSeparated, b"base".as_slice()),
            (ValueType::MergeSeparated, b"separated-a".as_slice()),
            (ValueType::Merge, b"-inline".as_slice()),
            (ValueType::MergeSeparated, b"separated-b".as_slice()),
        ],
    );
}

#[test]
fn test_value_merge_all_terminal_put_resets_pending_separated_chain() {
    let values = vec![
        Value::new(vec![Some(Column::new(
            ValueType::PutSeparated,
            b"old-base".as_slice(),
        ))]),
        Value::new(vec![Some(Column::new(
            ValueType::MergeSeparated,
            b"old-merge".as_slice(),
        ))]),
        Value::new(vec![Some(Column::new(
            ValueType::Put,
            b"new-base".as_slice(),
        ))]),
        Value::new(vec![Some(Column::new(
            ValueType::MergeSeparated,
            b"new-merge".as_slice(),
        ))]),
    ];

    let column = Value::try_merge_all_in_column_family(
        values.into_iter().map(Ok),
        Schema::empty().as_ref(),
        DEFAULT_COLUMN_FAMILY_ID,
        None,
    )
    .unwrap()
    .columns
    .into_iter()
    .next()
    .flatten()
    .expect("merged column");
    assert_separated_array(
        &column,
        ValueType::PutSeparatedArray,
        &[
            (ValueType::Put, b"new-base".as_slice()),
            (ValueType::MergeSeparated, b"new-merge".as_slice()),
        ],
    );
}

#[test]
fn test_value_merge_all_merge_after_delete_keeps_empty_base_semantics() {
    let values = vec![
        Value::new(vec![Some(Column::new(ValueType::Put, b"old".as_slice()))]),
        Value::new(vec![Some(Column::new(ValueType::Delete, Bytes::new()))]),
        Value::new(vec![Some(Column::new(
            ValueType::MergeSeparated,
            b"after-delete".as_slice(),
        ))]),
        Value::new(vec![Some(Column::new(
            ValueType::Merge,
            b"-tail".as_slice(),
        ))]),
    ];

    let column = merge_single_column(values, Schema::empty().as_ref());
    assert_separated_array(
        &column,
        ValueType::MergeSeparatedArray,
        &[
            (ValueType::MergeSeparated, b"after-delete".as_slice()),
            (ValueType::Merge, b"-tail".as_slice()),
        ],
    );
}

#[test]
fn test_column_merge_with_put_replaces() {
    let old = Column::new(ValueType::Put, b"old_data".to_vec());
    let new = Column::new(ValueType::Put, b"new_data".to_vec());

    let merged = old
        .merge(new, default_merge_operator_ref().as_ref(), None)
        .unwrap();
    assert!(matches!(merged.value_type(), ValueType::Put));
    assert_eq!(merged.data().as_ref(), b"new_data");
}

#[test]
fn test_column_merge_with_delete_replaces() {
    let old = Column::new(ValueType::Put, b"old_data".to_vec());
    let new = Column::new(ValueType::Delete, b"".to_vec());

    let merged = old
        .merge(new, default_merge_operator_ref().as_ref(), None)
        .unwrap();
    assert!(matches!(merged.value_type(), ValueType::Delete));
    assert_eq!(merged.data().as_ref(), b"");
}

#[test]
fn test_column_merge_with_merge_concatenates() {
    let old = Column::new(ValueType::Put, b"hello".to_vec());
    let new = Column::new(ValueType::Merge, b"world".to_vec());

    let merged = old
        .merge(new, default_merge_operator_ref().as_ref(), None)
        .unwrap();
    // Merge keeps the original value_type and concatenates data
    assert!(matches!(merged.value_type(), ValueType::Put));
    assert_eq!(merged.data().as_ref(), b"helloworld");
}

#[test]
fn test_column_merge_multiple_merges() {
    let old = Column::new(ValueType::Put, b"a".to_vec());
    let merge1 = Column::new(ValueType::Merge, b"b".to_vec());
    let merge2 = Column::new(ValueType::Merge, b"c".to_vec());

    let merged = old
        .merge(merge1, default_merge_operator_ref().as_ref(), None)
        .unwrap()
        .merge(merge2, default_merge_operator_ref().as_ref(), None)
        .unwrap();
    assert!(matches!(merged.value_type(), ValueType::Put));
    assert_eq!(merged.data().as_ref(), b"abc");
}

#[test]
fn test_value_merge_all_columns_present() {
    let old = Value::new(vec![
        Some(Column::new(ValueType::Put, b"old1".to_vec())),
        Some(Column::new(ValueType::Put, b"old2".to_vec())),
    ]);
    let new = Value::new(vec![
        Some(Column::new(ValueType::Put, b"new1".to_vec())),
        Some(Column::new(ValueType::Merge, b"_append".to_vec())),
    ]);

    let merged = old.merge(new, &Schema::empty(), None).unwrap();
    let cols = merged.columns();

    assert_eq!(cols.len(), 2);
    assert_eq!(cols[0].as_ref().unwrap().data().as_ref(), b"new1");
    assert_eq!(cols[1].as_ref().unwrap().data().as_ref(), b"old2_append");
}

#[test]
fn test_value_merge_partial_columns() {
    let old = Value::new(vec![
        Some(Column::new(ValueType::Put, b"old1".to_vec())),
        Some(Column::new(ValueType::Put, b"old2".to_vec())),
    ]);
    let new = Value::new(vec![
        None,
        Some(Column::new(ValueType::Put, b"new2".to_vec())),
    ]);

    let merged = old.merge(new, &Schema::empty(), None).unwrap();
    let cols = merged.columns();

    assert_eq!(cols.len(), 2);
    // First column unchanged
    assert_eq!(cols[0].as_ref().unwrap().data().as_ref(), b"old1");
    // Second column replaced
    assert_eq!(cols[1].as_ref().unwrap().data().as_ref(), b"new2");
}

#[test]
fn test_value_merge_new_column_fills_none() {
    let old = Value::new(vec![
        Some(Column::new(ValueType::Put, b"old1".to_vec())),
        None,
    ]);
    let new = Value::new(vec![
        None,
        Some(Column::new(ValueType::Put, b"new2".to_vec())),
    ]);

    let merged = old.merge(new, &Schema::empty(), None).unwrap();
    let cols = merged.columns();

    assert_eq!(cols.len(), 2);
    assert_eq!(cols[0].as_ref().unwrap().data().as_ref(), b"old1");
    assert_eq!(cols[1].as_ref().unwrap().data().as_ref(), b"new2");
}

#[test]
fn test_value_merge_different_lengths() {
    let old = Value::new(vec![Some(Column::new(ValueType::Put, b"old1".to_vec()))]);
    let new = Value::new(vec![
        None,
        Some(Column::new(ValueType::Put, b"new2".to_vec())),
        Some(Column::new(ValueType::Put, b"new3".to_vec())),
    ]);

    let merged = old.merge(new, &Schema::empty(), None).unwrap();
    let cols = merged.columns();

    assert_eq!(cols.len(), 3);
    assert_eq!(cols[0].as_ref().unwrap().data().as_ref(), b"old1");
    assert_eq!(cols[1].as_ref().unwrap().data().as_ref(), b"new2");
    assert_eq!(cols[2].as_ref().unwrap().data().as_ref(), b"new3");
}

#[test]
fn test_value_merge_all_none() {
    let old = Value::new(vec![None, None]);
    let new = Value::new(vec![None, None]);

    let merged = old.merge(new, &Schema::empty(), None).unwrap();
    let cols = merged.columns();

    assert_eq!(cols.len(), 2);
    assert!(cols[0].is_none());
    assert!(cols[1].is_none());
}

#[test]
fn test_encode_decode_merge_separated_array_flatten_nested() {
    let nested_columns = [
        Column::new(ValueType::PutSeparated, b"p1".to_vec()),
        Column::new(ValueType::MergeSeparated, b"m1".to_vec()),
    ];
    let nested_refs: Vec<_> = nested_columns.iter().map(Column::as_ref_column).collect();
    let nested = encode_merge_separated_array(&nested_refs).unwrap();
    let encoded_columns = [
        Column::new(ValueType::Put, b"inline".to_vec()),
        Column::new(ValueType::MergeSeparatedArray, nested),
        Column::new(ValueType::Merge, b"suffix".to_vec()),
    ];
    let encoded_refs: Vec<_> = encoded_columns.iter().map(Column::as_ref_column).collect();
    let encoded = encode_merge_separated_array(&encoded_refs).unwrap();
    let decoded = decode_merge_separated_array(&encoded).unwrap();
    assert_eq!(decoded.len(), 4);
    assert_eq!(decoded[0].value_type, ValueType::Put);
    assert_eq!(decoded[0].data(), b"inline");
    assert_eq!(decoded[1].value_type, ValueType::PutSeparated);
    assert_eq!(decoded[1].data(), b"p1");
    assert_eq!(decoded[2].value_type, ValueType::MergeSeparated);
    assert_eq!(decoded[2].data(), b"m1");
    assert_eq!(decoded[3].value_type, ValueType::Merge);
    assert_eq!(decoded[3].data(), b"suffix");
}

#[test]
fn test_decode_merge_separated_array_rejects_nested_type() {
    let mut invalid = BytesMut::new();
    invalid.put_u8(ValueType::MergeSeparatedArray.encode_tag());
    invalid.put_u32_le(3);
    invalid.put_slice(b"bad");
    assert!(decode_merge_separated_array(&invalid).is_err());
}

#[test]
fn test_column_merge_with_separated_creates_array() {
    let old = Column::new(ValueType::PutSeparated, b"p0".to_vec());
    let new = Column::new(ValueType::MergeSeparated, b"m1".to_vec());
    let merged = old
        .merge(new, default_merge_operator_ref().as_ref(), None)
        .unwrap();
    assert_eq!(merged.value_type, ValueType::PutSeparatedArray);
    let decoded = decode_merge_separated_array(merged.data()).unwrap();
    assert_eq!(decoded.len(), 2);
    assert_eq!(decoded[0].value_type, ValueType::PutSeparated);
    assert_eq!(decoded[0].data(), b"p0");
    assert_eq!(decoded[1].value_type, ValueType::MergeSeparated);
    assert_eq!(decoded[1].data(), b"m1");
}

#[test]
fn test_put_separated_is_terminal() {
    let value = Value::new(vec![Some(Column::new(
        ValueType::PutSeparated,
        b"p".to_vec(),
    ))]);
    assert!(value.is_terminal());
    assert_eq!(value.terminal_mask(), vec![0b0000_0001]);
}

#[test]
fn test_value_type_bit_semantics() {
    assert_eq!(ValueType::Put.encode_tag() & VALUE_TYPE_TERMINAL_BIT, 1);
    assert_eq!(
        ValueType::Merge.encode_tag() & VALUE_TYPE_MERGE_BIT,
        VALUE_TYPE_MERGE_BIT
    );
    assert_eq!(
        ValueType::PutSeparated.encode_tag() & VALUE_TYPE_SEPARATED_BIT,
        VALUE_TYPE_SEPARATED_BIT
    );
    assert_eq!(
        ValueType::MergeSeparatedArray.encode_tag() & VALUE_TYPE_ARRAY_BIT,
        VALUE_TYPE_ARRAY_BIT
    );
}

#[test]
fn test_value_merge_callback_invoked_with_empty_sides() {
    let old = Value::new(vec![
        Some(Column::new(ValueType::Put, b"old0".to_vec())),
        None,
    ]);
    let new = Value::new(vec![
        None,
        Some(Column::new(ValueType::PutSeparated, b"p1".to_vec())),
        None,
    ]);
    let mut seen = Vec::new();
    let _ = old
        .merge_with_callback(
            new,
            &Schema::empty(),
            DEFAULT_COLUMN_FAMILY_ID,
            None,
            &mut |old_col, new_col| {
                seen.push((old_col.map(|c| c.value_type), new_col.map(|c| c.value_type)));
            },
        )
        .unwrap();
    assert_eq!(
        seen,
        vec![
            (Some(ValueType::Put), None),
            (None, Some(ValueType::PutSeparated)),
        ]
    );
}

#[test]
fn test_value_merge_skips_operator_when_old_missing() {
    let old = Value::new(vec![None]);
    let new = Value::new(vec![Some(Column::new(ValueType::Merge, b"m".to_vec()))]);
    let schema = Schema::new(0, 1, vec![Arc::new(PanicMergeOperator)]);
    let merged = old.merge(new, &schema, None).unwrap();
    let col = merged.columns()[0].as_ref().unwrap();
    assert_eq!(col.value_type, ValueType::Merge);
    assert_eq!(col.data().as_ref(), b"m");
}

#[test]
fn test_value_merge_skips_operator_when_old_empty() {
    let old = Value::new(vec![Some(Column::new(ValueType::Put, Bytes::new()))]);
    let new = Value::new(vec![Some(Column::new(ValueType::Merge, b"m".to_vec()))]);
    let schema = Schema::new(0, 1, vec![Arc::new(PanicMergeOperator)]);
    let merged = old.merge(new, &schema, None).unwrap();
    let col = merged.columns()[0].as_ref().unwrap();
    assert_eq!(col.value_type, ValueType::Merge);
    assert_eq!(col.data().as_ref(), b"m");
}
