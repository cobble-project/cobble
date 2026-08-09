use super::*;

struct BracketMergeOperator;

impl MergeOperator for BracketMergeOperator {
    fn merge(
        &self,
        existing_value: Bytes,
        value: Bytes,
        _time_provider: Option<&dyn TimeProvider>,
    ) -> Result<(Bytes, Option<ValueType>)> {
        let existing = existing_value.as_ref();
        Ok((
            format!(
                "[{}+{}]",
                String::from_utf8_lossy(existing),
                String::from_utf8_lossy(value.as_ref())
            )
            .into_bytes()
            .into(),
            None,
        ))
    }
}

#[test]
fn test_bytes_merge_operator_merge() {
    let op = BytesMergeOperator;
    let (merged, _) = op
        .merge(Bytes::from_static(b"old"), Bytes::from_static(b"new"), None)
        .unwrap();
    assert_eq!(merged.as_ref(), b"oldnew");
}

#[test]
fn test_bytes_merge_operator_merge_batch() {
    let op = BytesMergeOperator;
    let (merged, _) = op
        .merge_batch(
            Bytes::from_static(b"base"),
            vec![
                Bytes::from_static(b"a"),
                Bytes::from_static(b"b"),
                Bytes::from_static(b"c"),
            ],
            None,
        )
        .unwrap();
    assert_eq!(merged.as_ref(), b"baseabc");
}

#[test]
fn test_merge_operator_default_batch_uses_merge() {
    let op = BracketMergeOperator;
    let (merged, _) = op
        .merge_batch(
            Bytes::from_static(b"base"),
            vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")],
            None,
        )
        .unwrap();
    assert_eq!(merged.as_ref(), b"[[base+a]+b]");
}

#[test]
fn test_u32_counter_merge_operator() {
    let op = U32CounterMergeOperator;
    let (merged, _) = op
        .merge(
            Bytes::copy_from_slice(&3u32.to_le_bytes()),
            Bytes::copy_from_slice(&4u32.to_le_bytes()),
            None,
        )
        .unwrap();
    let value = u32::from_le_bytes(merged.as_ref().try_into().unwrap());
    assert_eq!(value, 7);
}

#[test]
fn test_u64_counter_merge_operator() {
    let op = U64CounterMergeOperator;
    let (merged, _) = op
        .merge_batch(
            Bytes::copy_from_slice(&1u64.to_le_bytes()),
            vec![
                Bytes::copy_from_slice(&2u64.to_le_bytes()),
                Bytes::copy_from_slice(&3u64.to_le_bytes()),
            ],
            None,
        )
        .unwrap();
    let value = u64::from_le_bytes(merged.as_ref().try_into().unwrap());
    assert_eq!(value, 6);
}
