use super::*;

#[test]
fn test_list_round_trip() {
    let config = ListConfig {
        max_elements: Some(2),
        retain_mode: ListRetainMode::Last,
        preserve_element_ttl: false,
    };
    let encoded = encode_list_for_write(
        vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")],
        &config,
        None,
        10,
    )
    .unwrap();
    let decoded = decode_list_for_read(&encoded, &config, 10).unwrap();
    assert_eq!(
        decoded,
        vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")]
    );
}

#[test]
fn test_list_ttl_uses_supplied_time() {
    let config = ListConfig {
        max_elements: None,
        retain_mode: ListRetainMode::Last,
        preserve_element_ttl: true,
    };
    let encoded =
        encode_list_for_write(vec![Bytes::from_static(b"a")], &config, Some(5), 100).unwrap();
    assert_eq!(
        decode_list_for_read(&encoded, &config, 104).unwrap(),
        vec![Bytes::from_static(b"a")]
    );
    assert!(
        decode_list_for_read(&encoded, &config, 105)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn test_merge_batch_fast_append_keeps_valid_payload() {
    let config = ListConfig {
        max_elements: Some(4),
        retain_mode: ListRetainMode::Last,
        preserve_element_ttl: false,
    };
    let operator = ListMergeOperator::new(config.clone());
    let left = encode_list_for_write(
        vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")],
        &config,
        None,
        0,
    )
    .unwrap();
    let right = encode_list_for_write(vec![Bytes::from_static(b"c")], &config, None, 0).unwrap();
    let merged = operator.merge_batch(left, vec![right], None).unwrap();
    assert_eq!(
        decode_list_for_read(&merged.0, &config, 0).unwrap(),
        vec![
            Bytes::from_static(b"a"),
            Bytes::from_static(b"b"),
            Bytes::from_static(b"c"),
        ]
    );
}

#[test]
fn test_merge_over_cap_falls_back_to_retain_policy() {
    let config = ListConfig {
        max_elements: Some(2),
        retain_mode: ListRetainMode::Last,
        preserve_element_ttl: false,
    };
    let operator = ListMergeOperator::new(config.clone());
    let left = encode_list_for_write(vec![Bytes::from_static(b"a")], &config, None, 0).unwrap();
    let right = encode_list_for_write(
        vec![Bytes::from_static(b"b"), Bytes::from_static(b"c")],
        &config,
        None,
        0,
    )
    .unwrap();
    let merged = operator.merge_batch(left, vec![right], None).unwrap();
    assert_eq!(
        decode_list_for_read(&merged.0, &config, 0).unwrap(),
        vec![Bytes::from_static(b"b"), Bytes::from_static(b"c")]
    );
}

#[test]
fn test_merge_last_cap_returns_put_value_type() {
    let config = ListConfig {
        max_elements: Some(2),
        retain_mode: ListRetainMode::Last,
        preserve_element_ttl: false,
    };
    let operator = ListMergeOperator::new(config.clone());
    let left = encode_list_for_write(vec![Bytes::from_static(b"a")], &config, None, 0).unwrap();
    let right = encode_list_for_write(
        vec![Bytes::from_static(b"b"), Bytes::from_static(b"c")],
        &config,
        None,
        0,
    )
    .unwrap();
    let merged = operator.merge_batch(left, vec![right], None).unwrap();
    assert_eq!(merged.1, Some(ValueType::Put));
}

#[test]
fn test_merge_last_stops_before_older_payloads() {
    let config = ListConfig {
        max_elements: Some(2),
        retain_mode: ListRetainMode::Last,
        preserve_element_ttl: true,
    };
    let operator = ListMergeOperator::new(config.clone());
    // Older payload is malformed; merge should still succeed because retain-last stops after
    // consuming enough elements from newer operands.
    let malformed_existing = Bytes::from_static(b"\x01");
    let op1 = encode_list_for_write(vec![Bytes::from_static(b"a")], &config, None, 0).unwrap();
    let op2 = encode_list_for_write(vec![Bytes::from_static(b"b")], &config, None, 0).unwrap();
    let merged = operator
        .merge_batch(malformed_existing, vec![op1, op2], None)
        .unwrap();
    assert_eq!(
        decode_list_for_read(&merged.0, &config, 0).unwrap(),
        vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")]
    );
}

#[test]
fn test_merge_first_cap_stops_and_keeps_merge_type() {
    let config = ListConfig {
        max_elements: Some(2),
        retain_mode: ListRetainMode::First,
        preserve_element_ttl: false,
    };
    let operator = ListMergeOperator::new(config.clone());
    let left = encode_list_for_write(vec![Bytes::from_static(b"a")], &config, None, 0).unwrap();
    let right = encode_list_for_write(
        vec![
            Bytes::from_static(b"b"),
            Bytes::from_static(b"c"),
            Bytes::from_static(b"d"),
        ],
        &config,
        None,
        0,
    )
    .unwrap();
    let merged = operator.merge_batch(left, vec![right], None).unwrap();
    assert_eq!(merged.1, None);
    assert_eq!(
        decode_list_for_read(&merged.0, &config, 0).unwrap(),
        vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")]
    );
}

#[test]
fn test_list_operator_from_metadata_requires_metadata() {
    assert!(list_operator_from_metadata(LIST_OPERATOR_ID, None).is_none());
}
