use super::*;
use crate::iterator::KvIterator;
use crate::r#type::{RefColumn, ValueType};

fn collect_values(mem: &SkiplistMemtable, key: &[u8]) -> Vec<Vec<u8>> {
    let mut values = Vec::new();
    assert!(
        mem.visit_values_until(key, |value| {
            values.push(value.to_vec());
            Ok(ControlFlow::Continue(()))
        })
        .unwrap()
        .is_continue()
    );
    values
}

#[test]
fn visitor_matches_version_order_and_stops_at_key_boundary() {
    let mut mem = SkiplistMemtable::with_capacity(4096);
    mem.put(b"a", b"neighbor-before").unwrap();
    mem.put(b"key", b"base").unwrap();
    mem.put(b"key", b"merge-one").unwrap();
    mem.put(b"key", b"merge-two").unwrap();
    mem.put(b"z", b"neighbor-after").unwrap();
    for (key, expected) in [
        (
            b"key".as_slice(),
            vec![b"merge-two".as_slice(), b"merge-one", b"base"],
        ),
        (b"a", vec![b"neighbor-before".as_slice()]),
        (b"z", vec![b"neighbor-after".as_slice()]),
        (b"missing", vec![]),
        (b"", vec![]),
        (b"zz", vec![]),
    ] {
        assert_eq!(collect_values(&mem, key), expected);
    }
}

#[test]
fn visitor_break_and_error_do_not_visit_older_versions() {
    let mut mem = SkiplistMemtable::with_capacity(4096);
    mem.put(b"key", b"old").unwrap();
    mem.put(b"key", b"new").unwrap();
    let mut visits = 0;
    assert!(
        mem.visit_values_until(b"key", |value| {
            visits += 1;
            assert_eq!(value, b"new");
            Ok(std::ops::ControlFlow::Break(()))
        })
        .unwrap()
        .is_break()
    );
    assert_eq!(visits, 1);
    let result = mem.visit_values_until(b"key", |_| {
        visits += 1;
        Err(Error::InvalidState("visitor failed".into()))
    });
    assert!(matches!(result, Err(Error::InvalidState(message)) if message == "visitor failed"));
    assert_eq!(visits, 2);
}

#[test]
fn inplace_replace_latest_requires_equal_length_and_unsealed_entry() {
    let mut mem = SkiplistMemtable::with_capacity(1024);
    let key = RefKey::new(0, b"key");
    let old = RefValue::new(vec![Some(RefColumn::new(ValueType::Put, b"old"))]);
    let new = RefValue::new(vec![Some(RefColumn::new(ValueType::Put, b"new"))]);
    let shorter = RefValue::new(vec![Some(RefColumn::new(ValueType::Put, b"x"))]);
    mem.put_ref(&key, &old, 1).unwrap();
    let offset = mem.data_offset();
    mem.put_ref_or_replace(&key, &new, 1, 0).unwrap();
    assert_eq!(mem.data_offset(), offset);
    let mut encoded_key = vec![0; key.encoded_len()];
    encode_key_ref_into(&key, &mut encoded_key.as_mut_slice());
    let mut expected_value = vec![0; new.encoded_len(1)];
    encode_value_ref_into(&new, 1, &mut expected_value.as_mut_slice());
    assert_eq!(mem.get(&encoded_key), Some(expected_value.as_slice()));
    mem.put_ref_or_replace(&key, &shorter, 1, 0).unwrap();
    let shorter_offset = mem.data_offset();
    assert!(shorter_offset > offset);
    mem.put_ref_or_replace(&key, &new, 1, 0).unwrap();
    let longer_offset = mem.data_offset();
    assert!(longer_offset > shorter_offset);
    mem.put_ref_or_replace(&key, &new, 1, longer_offset)
        .unwrap();
    assert!(mem.data_offset() > longer_offset);
    assert_eq!(collect_values(&mem, &encoded_key).len(), 4);
    assert_eq!(mem.get(&encoded_key), Some(expected_value.as_slice()));
}

#[test]
fn combined_missing_insert_matches_append_order_and_encoding() {
    let mut combined = SkiplistMemtable::with_capacity(64 * 1024);
    let mut reference = SkiplistMemtable::with_capacity(64 * 1024);
    let value = RefValue::new(vec![Some(RefColumn::new(ValueType::Put, b"value"))]);
    for index in 0..257 {
        let key_bytes = format!("shared-prefix-0123456789-{:03}", index * 73 % 257);
        let key = RefKey::new((index % 3) as u16, key_bytes.as_bytes());
        combined.put_ref_or_replace(&key, &value, 1, 0).unwrap();
        reference.put_ref(&key, &value, 1).unwrap();
    }
    assert_eq!(combined.data_end, reference.data_end);
    assert_eq!(combined.next_ordinal, reference.next_ordinal);
    assert_eq!(combined.buffer, reference.buffer);
    let mut combined_iter = combined.iter();
    let mut reference_iter = reference.iter();
    combined_iter.seek_to_first().unwrap();
    reference_iter.seek_to_first().unwrap();
    while reference_iter.next().unwrap() {
        assert!(combined_iter.next().unwrap());
        assert_eq!(combined_iter.key().unwrap(), reference_iter.key().unwrap());
        assert_eq!(
            combined_iter
                .take_value()
                .unwrap()
                .unwrap()
                .unwrap_encoded(),
            reference_iter
                .take_value()
                .unwrap()
                .unwrap()
                .unwrap_encoded()
        );
    }
    assert!(!combined_iter.next().unwrap());
}

#[test]
fn combined_insert_preserves_sealed_prefix_and_replaces_at_seal_boundary() {
    let mut mem = SkiplistMemtable::with_capacity(1024);
    let key = RefKey::new(0, b"key");
    let old = RefValue::new(vec![Some(RefColumn::new(ValueType::Put, b"old"))]);
    let new = RefValue::new(vec![Some(RefColumn::new(ValueType::Put, b"new"))]);
    let newest = RefValue::new(vec![Some(RefColumn::new(ValueType::Put, b"end"))]);
    mem.put_ref_or_replace(&key, &old, 1, 0).unwrap();
    let seal = mem.data_offset();
    let sealed_bytes = mem.buffer[..seal].to_vec();
    mem.put_ref_or_replace(&key, &new, 1, seal).unwrap();
    let after_append = mem.data_offset();
    assert!(after_append > seal);
    assert_eq!(&mem.buffer[..seal], sealed_bytes);
    mem.put_ref_or_replace(&key, &newest, 1, seal).unwrap();
    assert_eq!(mem.data_offset(), after_append);
    assert_eq!(&mem.buffer[..seal], sealed_bytes);

    let mut encoded_key = vec![0; key.encoded_len()];
    encode_key_ref_into(&key, &mut encoded_key.as_mut_slice());
    let node = mem.lower_bound_node(&encoded_key);
    assert_eq!(mem.node_entry_offset(node), Some(seal));
    let values: Vec<_> = collect_values(&mem, &encoded_key)
        .iter()
        .map(|bytes| {
            crate::sst::row_codec::decode_value(&mut Bytes::copy_from_slice(bytes), 1).unwrap()
        })
        .collect();
    assert_eq!(values.len(), 2);
    assert_eq!(
        values[0].columns()[0].as_ref().unwrap().data().as_ref(),
        b"end"
    );
    assert_eq!(
        values[1].columns()[0].as_ref().unwrap().data().as_ref(),
        b"old"
    );
}

#[test]
fn combined_full_table_replaces_but_failed_insert_leaves_all_state_unchanged() {
    let key = RefKey::new(0, b"key");
    let old = RefValue::new(vec![Some(RefColumn::new(ValueType::Put, b"old"))]);
    let new = RefValue::new(vec![Some(RefColumn::new(ValueType::Put, b"new"))]);
    let capacity = SkiplistMemtable::entry_size(key.encoded_len(), old.encoded_len(1))
        + SkiplistMemtable::node_size(SkiplistMemtable::choose_height(0));
    let mut mem = SkiplistMemtable::with_capacity(capacity);
    mem.put_ref_or_replace(&key, &old, 1, 0).unwrap();
    assert_eq!(mem.remaining_capacity(), 0);
    mem.put_ref_or_replace(&key, &new, 1, 0).unwrap();
    let before = (
        mem.data_end,
        mem.arena_cursor,
        mem.heads,
        mem.max_height,
        mem.next_ordinal,
    );
    let before_buffer = mem.buffer.clone();
    for (failed_key, seal) in [(RefKey::new(0, b"missing"), 0), (key, mem.data_offset())] {
        assert!(matches!(
            mem.put_ref_or_replace(&failed_key, &new, 1, seal),
            Err(Error::MemtableFull { .. })
        ));
        assert_eq!(
            (
                mem.data_end,
                mem.arena_cursor,
                mem.heads,
                mem.max_height,
                mem.next_ordinal
            ),
            before
        );
        assert_eq!(mem.buffer, before_buffer);
    }
}

#[test]
fn combined_multi_column_replacement_preserves_ttl_and_encoded_length_rules() {
    let mut mem = SkiplistMemtable::with_capacity(1024);
    let key = RefKey::new(0, b"key");
    let old = RefValue::new_with_expired_at(
        vec![
            Some(RefColumn::new(ValueType::Put, b"aa")),
            Some(RefColumn::new(ValueType::Put, b"bbbb")),
        ],
        Some(100),
    );
    let new = RefValue::new_with_expired_at(
        vec![
            Some(RefColumn::new(ValueType::Put, b"cc")),
            Some(RefColumn::new(ValueType::Put, b"dddd")),
        ],
        Some(200),
    );
    mem.put_ref_or_replace(&key, &old, 2, 0).unwrap();
    let offset = mem.data_offset();
    mem.put_ref_or_replace(&key, &new, 2, 0).unwrap();
    assert_eq!(mem.data_offset(), offset);
    let mut encoded_key = vec![0; key.encoded_len()];
    encode_key_ref_into(&key, &mut encoded_key.as_mut_slice());
    let mut encoded = Bytes::copy_from_slice(mem.get(&encoded_key).unwrap());
    let decoded = crate::sst::row_codec::decode_value(&mut encoded, 2).unwrap();
    assert_eq!(decoded.expired_at(), Some(200));
    assert_eq!(
        decoded.columns()[0].as_ref().unwrap().data().as_ref(),
        b"cc"
    );
    assert_eq!(
        decoded.columns()[1].as_ref().unwrap().data().as_ref(),
        b"dddd"
    );
    let longer = RefValue::new_with_expired_at(
        vec![
            Some(RefColumn::new(ValueType::Put, b"longer")),
            Some(RefColumn::new(ValueType::Put, b"dddd")),
        ],
        Some(300),
    );
    mem.put_ref_or_replace(&key, &longer, 2, 0).unwrap();
    assert!(mem.data_offset() > offset);
    assert_eq!(collect_values(&mem, &encoded_key).len(), 2);
}

fn assert_compare_matches_full_key_ordering(keys: &[&[u8]]) {
    let mut mem = SkiplistMemtable::with_capacity(8192);
    for &key in keys {
        mem.put(key, b"value").unwrap();
    }

    for &stored_key in keys {
        let node = mem.lower_bound_node(stored_key);
        assert_ne!(node, NULL_OFFSET);
        assert_eq!(mem.node_key(node), Some(stored_key));
        for &target in keys {
            assert_eq!(
                mem.compare_node_key(node, target),
                Some(stored_key.cmp(target)),
                "stored {:?}, target {:?}",
                stored_key,
                target
            );
        }
    }
}

#[test]
fn put_and_get() {
    let mut mem = SkiplistMemtable::with_capacity(1024);
    mem.put(b"key1", b"value1").unwrap();
    mem.put(b"key2", b"value2").unwrap();
    assert_eq!(mem.get(b"key1").unwrap(), b"value1");
    assert_eq!(mem.get(b"key2").unwrap(), b"value2");
    assert!(mem.get(b"missing").is_none());
}

#[test]
fn visitor_returns_latest_first() {
    let mut mem = SkiplistMemtable::with_capacity(2048);
    mem.put(b"key", b"v1").unwrap();
    mem.put(b"key", b"v2").unwrap();
    mem.put(b"key", b"v3").unwrap();
    let mut iter = collect_values(&mem, b"key").into_iter();
    assert_eq!(iter.next().unwrap(), b"v3");
    assert_eq!(iter.next().unwrap(), b"v2");
    assert_eq!(iter.next().unwrap(), b"v1");
    assert!(iter.next().is_none());
}

#[test]
fn kv_iterator_orders_keys_and_values() {
    let mut mem = SkiplistMemtable::with_capacity(4096);
    mem.put(b"b", b"v1").unwrap();
    mem.put(b"a", b"x1").unwrap();
    mem.put(b"a", b"x2").unwrap();
    mem.put(b"c", b"z1").unwrap();
    let mut iter = mem.iter();
    iter.seek_to_first().unwrap();
    let mut collected = Vec::new();
    while iter.next().unwrap() {
        let k = iter.take_key().unwrap().unwrap();
        let v = iter.take_value().unwrap().unwrap().unwrap_encoded();
        collected.push((k, v));
    }
    let expected: Vec<(&[u8], &[u8])> =
        vec![(b"a", b"x2"), (b"a", b"x1"), (b"b", b"v1"), (b"c", b"z1")];
    assert_eq!(collected.len(), expected.len());
    for (got, exp) in collected.iter().zip(expected.iter()) {
        assert_eq!(got.0.as_ref(), exp.0);
        assert_eq!(got.1.as_ref(), exp.1);
    }
}

#[test]
fn capacity_enforced() {
    let mut mem = SkiplistMemtable::with_capacity(64);
    mem.put(b"k1", b"v1").unwrap();
    let err = mem.put(b"k2", b"value_too_long").unwrap_err();
    match err {
        Error::MemtableFull { .. } => {}
        _ => panic!("unexpected error type"),
    }
}

#[test]
fn predecessor_search_matches_lower_bound_for_missing_and_present_keys() {
    let mut mem = SkiplistMemtable::with_capacity(8192);
    for (key, value) in [
        (b"aa".as_slice(), b"v1".as_slice()),
        (b"ab", b"v2"),
        (b"ac", b"v3"),
        (b"b", b"v4"),
        (b"ba", b"v5"),
        (b"c", b"v6"),
    ] {
        mem.put(key, value).unwrap();
    }

    for (target, expected_pred, expected_lower_bound) in [
        (b"a".as_slice(), None, Some(b"aa".as_slice())),
        (b"aa", None, Some(b"aa")),
        (b"aad", Some(b"aa".as_slice()), Some(b"ab".as_slice())),
        (b"ab", Some(b"aa".as_slice()), Some(b"ab".as_slice())),
        (b"ad", Some(b"ac".as_slice()), Some(b"b".as_slice())),
        (b"bb", Some(b"ba".as_slice()), Some(b"c".as_slice())),
        (b"d", Some(b"c".as_slice()), None),
    ] {
        let lower_bound = mem.lower_bound_node(target);
        let lower_bound_key = if lower_bound == NULL_OFFSET {
            None
        } else {
            Some(mem.node_entry(lower_bound).unwrap().0)
        };
        assert_eq!(lower_bound_key, expected_lower_bound, "target {:?}", target);

        let update = mem.find_predecessors_for_key(target);
        let predecessor = update[0];
        let predecessor_key = if predecessor == NULL_OFFSET {
            None
        } else {
            Some(mem.node_entry(predecessor).unwrap().0)
        };
        assert_eq!(predecessor_key, expected_pred, "target {:?}", target);

        let derived_lower_bound = if predecessor == NULL_OFFSET {
            mem.heads[0]
        } else {
            mem.node_next(predecessor, 0).unwrap_or(NULL_OFFSET)
        };
        assert_eq!(derived_lower_bound, lower_bound, "target {:?}", target);
    }
}

#[test]
fn cached_prefix_comparison_matches_full_key_ordering() {
    let ascii_keys: [&[u8]; 9] = [
        b"1234567".as_slice(),
        b"12345678",
        b"123456789",
        b"shared08-a",
        b"shared08-z",
        b"0123456789abcde",
        b"0123456789abcdef",
        b"0123456789abcdef-a",
        b"0123456789abcdef-z",
    ];
    let eight_byte_keys = [
        [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
        [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x7f],
        [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80],
        [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff],
        [0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
        [0xff, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
    ];
    let key = |first, second| {
        let mut key = [0u8; NODE_KEY_PREFIX_SIZE];
        key[0] = first;
        key[U64_PREFIX_SIZE] = second;
        key
    };
    let sixteen_byte_keys = [
        key(0x00, 0x00),
        key(0x7f, 0x00),
        key(0x80, 0x00),
        key(0xff, 0x00),
        key(0x40, 0x00),
        key(0x40, 0x7f),
        key(0x40, 0x80),
        key(0x40, 0xff),
    ];
    let mut keys = ascii_keys.to_vec();
    keys.extend(eight_byte_keys.iter().map(|key| key.as_slice()));
    keys.extend(sixteen_byte_keys.iter().map(|key| key.as_slice()));
    assert_compare_matches_full_key_ordering(&keys);
}

#[test]
fn prefix_comparison_preserves_lower_bound_put_get_and_iteration_order() {
    let entries = [
        (b"0123456789abcdef-z".as_slice(), b"z1".as_slice()),
        (b"123456789", b"nine"),
        (b"shared08-z", b"z"),
        (b"12345678", b"eight"),
        (b"0123456789abcdef", b"sixteen"),
        (b"shared08-a", b"a"),
        (b"1234567", b"seven"),
        (b"0123456789abcdef-a", b"a1"),
        (b"12345678", b"latest-eight"),
    ];
    let mut mem = SkiplistMemtable::with_capacity(8192);
    for (key, value) in entries {
        mem.put(key, value).unwrap();
    }

    assert_eq!(mem.get(b"12345678"), Some(b"latest-eight".as_slice()));
    assert_eq!(mem.get(b"123456789"), Some(b"nine".as_slice()));
    assert_eq!(mem.get(b"shared08-a"), Some(b"a".as_slice()));

    for (target, expected) in [
        (b"1234567".as_slice(), Some(b"1234567".as_slice())),
        (b"1234567\0", Some(b"12345678".as_slice())),
        (b"12345678\x01", Some(b"123456789".as_slice())),
        (b"shared08-m", Some(b"shared08-z".as_slice())),
        (
            b"0123456789abcdef-y",
            Some(b"0123456789abcdef-z".as_slice()),
        ),
        (b"zzzz", None),
    ] {
        let node = mem.lower_bound_node(target);
        let found = if node == NULL_OFFSET {
            None
        } else {
            mem.node_key(node)
        };
        assert_eq!(found, expected, "target {:?}", target);
    }

    let mut iter = mem.iter();
    iter.seek_to_first().unwrap();
    let mut keys = Vec::new();
    while iter.next().unwrap() {
        keys.push(iter.take_key().unwrap().unwrap());
    }
    assert_eq!(
        keys,
        vec![
            Bytes::from_static(b"0123456789abcdef"),
            Bytes::from_static(b"0123456789abcdef-a"),
            Bytes::from_static(b"0123456789abcdef-z"),
            Bytes::from_static(b"1234567"),
            Bytes::from_static(b"12345678"),
            Bytes::from_static(b"12345678"),
            Bytes::from_static(b"123456789"),
            Bytes::from_static(b"shared08-a"),
            Bytes::from_static(b"shared08-z"),
        ]
    );
}

#[test]
fn large_scale_put_get_and_iteration_order() {
    const ENTRY_COUNT: usize = 20_000;
    let mut mem = SkiplistMemtable::with_capacity(16 * 1024 * 1024);
    for i in 0..ENTRY_COUNT {
        let key_id = (i * 11939 + 7) % ENTRY_COUNT;
        let key = format!("k{:08}", key_id);
        let value = format!("v{:08}", key_id);
        mem.put(key.as_bytes(), value.as_bytes()).unwrap();
    }

    for i in 0..ENTRY_COUNT {
        let key = format!("k{:08}", i);
        let expected = format!("v{:08}", i);
        assert_eq!(mem.get(key.as_bytes()).unwrap(), expected.as_bytes());
    }

    let mut iter = mem.iter();
    iter.seek_to_first().unwrap();
    let mut last_key = Vec::<u8>::new();
    let mut seen = 0usize;
    while iter.next().unwrap() {
        let key = iter.take_key().unwrap().unwrap();
        if !last_key.is_empty() {
            assert!(last_key.as_slice() <= key.as_ref());
        }
        last_key.clear();
        last_key.extend_from_slice(key.as_ref());
        seen += 1;
    }
    assert_eq!(seen, ENTRY_COUNT);
}

#[test]
fn large_scale_overwrite_keeps_latest_and_full_history() {
    const KEY_COUNT: usize = 2_000;
    const VERSIONS_PER_KEY: usize = 10;
    let mut mem = SkiplistMemtable::with_capacity(16 * 1024 * 1024);

    for version in 0..VERSIONS_PER_KEY {
        for key_id in 0..KEY_COUNT {
            let key = format!("k{:05}", key_id);
            let value = format!("v{:02}-{:05}", version, key_id);
            mem.put(key.as_bytes(), value.as_bytes()).unwrap();
        }
    }

    for key_id in [0usize, 17, 311, 1023, KEY_COUNT - 1] {
        let key = format!("k{:05}", key_id);
        let latest = format!("v{:02}-{:05}", VERSIONS_PER_KEY - 1, key_id);
        assert_eq!(mem.get(key.as_bytes()).unwrap(), latest.as_bytes());

        let collected = collect_values(&mem, key.as_bytes())
            .into_iter()
            .map(|v| String::from_utf8(v).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(collected.len(), VERSIONS_PER_KEY);
        for (idx, value) in collected.iter().enumerate() {
            let expected_version = VERSIONS_PER_KEY - 1 - idx;
            assert_eq!(*value, format!("v{:02}-{:05}", expected_version, key_id));
        }
    }
}
