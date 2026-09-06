use super::*;

#[test]
fn test_list_element_transform_preserves_layout_and_propagates_errors() {
    use crate::StructuredColumnType;
    use std::sync::atomic::{AtomicUsize, Ordering};

    for preserve_element_ttl in [false, true] {
        let config = ListConfig {
            max_elements: Some(1), // The adapter must not apply this cap.
            preserve_element_ttl,
            ..ListConfig::default()
        };
        let calls = Arc::new(AtomicUsize::new(0));
        let count = Arc::clone(&calls);
        let transform =
            StructuredColumnType::list_element_transform(config.clone(), move |value| {
                count.fetch_add(1, Ordering::Relaxed);
                let mut bytes = value.to_vec();
                bytes.push(b'!');
                Ok(Bytes::from(bytes))
            });
        assert_eq!(transform(None).unwrap(), None);
        assert_eq!(transform(Some(Bytes::new())).unwrap(), Some(Bytes::new()));
        let empty = encode_list_payload(&[], &config).unwrap();
        assert_eq!(transform(Some(empty.clone())).unwrap(), Some(empty));
        assert_eq!(calls.load(Ordering::Relaxed), 0);

        let mut elements = vec![
            DecodedListElement {
                value: Bytes::from_static(b"a"),
                expires_at_secs: Some(5),
            },
            DecodedListElement {
                value: Bytes::new(),
                expires_at_secs: None,
            },
            DecodedListElement {
                value: Bytes::from_static(b"xyz"),
                expires_at_secs: Some(90),
            },
        ];
        let payload = encode_list_payload(&elements, &config).unwrap();
        let output = transform(Some(payload.clone())).unwrap().unwrap();
        assert_eq!(calls.load(Ordering::Relaxed), 3);
        for element in &mut elements {
            let mut value = element.value.to_vec();
            value.push(b'!');
            element.value = Bytes::from(value);
        }
        // Exact encoding equality covers order/count, empty elements and every TTL.
        assert_eq!(output, encode_list_payload(&elements, &config).unwrap());

        let mut trailing = payload.to_vec();
        trailing.push(0);
        for corrupt in [
            Bytes::from_static(b"\x01"),
            payload.slice(..payload.len() - 1),
            Bytes::from(trailing),
        ] {
            assert!(transform(Some(corrupt)).is_err());
        }
        let failing = StructuredColumnType::list_element_transform(config, |_| {
            Err(Error::InputError("element transform failed".into()))
        });
        assert!(
            matches!(failing(Some(payload)), Err(Error::InputError(message)) if message == "element transform failed")
        );
    }
}

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
