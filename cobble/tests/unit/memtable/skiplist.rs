use super::*;
use crate::iterator::KvIterator;
use crate::r#type::{RefColumn, ValueType};

#[test]
fn inplace_replace_latest_requires_equal_length_and_unsealed_entry() {
    let mut mem = SkiplistMemtable::with_capacity(1024);
    let key = RefKey::new(0, b"key");
    let old = RefValue::new(vec![Some(RefColumn::new(ValueType::Put, b"old"))]);
    let new = RefValue::new(vec![Some(RefColumn::new(ValueType::Put, b"new"))]);
    let shorter = RefValue::new(vec![Some(RefColumn::new(ValueType::Put, b"x"))]);
    mem.put_ref(&key, &old, 1).unwrap();
    let offset = mem.data_offset();
    assert!(mem.try_replace_latest_ref(&key, &new, 1, 0).unwrap());
    assert_eq!(mem.data_offset(), offset);
    let mut encoded_key = vec![0; key.encoded_len()];
    encode_key_ref_into(&key, &mut encoded_key.as_mut_slice());
    let mut expected_value = vec![0; new.encoded_len(1)];
    encode_value_ref_into(&new, 1, &mut expected_value.as_mut_slice());
    assert_eq!(mem.get(&encoded_key), Some(expected_value.as_slice()));
    assert!(!mem.try_replace_latest_ref(&key, &shorter, 1, 0).unwrap());
    assert_eq!(mem.data_offset(), offset);
    assert!(!mem.try_replace_latest_ref(&key, &new, 1, offset).unwrap());
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
fn get_all_returns_latest_first() {
    let mut mem = SkiplistMemtable::with_capacity(2048);
    mem.put(b"key", b"v1").unwrap();
    mem.put(b"key", b"v2").unwrap();
    mem.put(b"key", b"v3").unwrap();
    let mut iter = mem.get_all(b"key");
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

        let collected = mem
            .get_all(key.as_bytes())
            .map(|v| String::from_utf8(v.to_vec()).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(collected.len(), VERSIONS_PER_KEY);
        for (idx, value) in collected.iter().enumerate() {
            let expected_version = VERSIONS_PER_KEY - 1 - idx;
            assert_eq!(*value, format!("v{:02}-{:05}", expected_version, key_id));
        }
    }
}
