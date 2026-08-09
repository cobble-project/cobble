use super::*;
use crate::iterator::KvIterator;
use crate::r#type::{RefColumn, RefKey, RefValue, ValueType};

#[test]
fn inplace_replace_last_entry_can_shrink_but_not_rewrite_an_older_entry() {
    let mut mem = VecMemtable::with_capacity(1024);
    let key = RefKey::new(0, b"key");
    let old = RefValue::new(vec![Some(RefColumn::new(ValueType::Put, b"long"))]);
    let short = RefValue::new(vec![Some(RefColumn::new(ValueType::Put, b"x"))]);
    mem.put_ref(&key, &old, 1).unwrap();
    let before = mem.data_offset();
    assert!(mem.try_replace_latest_ref(&key, &short, 1, 0).unwrap());
    assert!(mem.data_offset() < before);
    let mut encoded_key = vec![0; key.encoded_len()];
    encode_key_ref_into(&key, &mut encoded_key.as_mut_slice());
    let mut expected_value = vec![0; short.encoded_len(1)];
    encode_value_ref_into(&short, 1, &mut expected_value.as_mut_slice());
    assert_eq!(mem.get(&encoded_key), Some(expected_value.as_slice()));
    let other_key = RefKey::new(0, b"other");
    let other = RefValue::new(vec![Some(RefColumn::new(ValueType::Put, b"value"))]);
    mem.put_ref(&other_key, &other, 1).unwrap();
    let after_append = mem.data_offset();
    assert!(!mem.try_replace_latest_ref(&key, &old, 1, 0).unwrap());
    assert_eq!(mem.data_offset(), after_append);
}

#[test]
fn put_and_get() {
    let mut mem = VecMemtable::with_capacity(1024);
    mem.put(b"key1", b"value1").unwrap();
    mem.put(b"key2", b"value2").unwrap();

    assert_eq!(mem.get(b"key1").unwrap(), b"value1");
    assert_eq!(mem.get(b"key2").unwrap(), b"value2");
    assert!(mem.get(b"missing").is_none());
}

#[test]
fn overwrite_updates_value() {
    let mut mem = VecMemtable::with_capacity(1024);
    mem.put(b"key", b"old").unwrap();
    mem.put(b"key", b"new").unwrap();
    assert_eq!(mem.get(b"key").unwrap(), b"new");
}

#[test]
fn capacity_enforced() {
    let mut mem = VecMemtable::with_capacity(24);
    mem.put(b"k1", b"v1").unwrap();
    let err = mem.put(b"k2", b"value_too_long").unwrap_err();
    match err {
        Error::MemtableFull { .. } => {}
        _ => panic!("unexpected error type"),
    }
}

#[test]
fn get_all_returns_latest_first() {
    let mut mem = VecMemtable::with_capacity(512);
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
    let mut mem = VecMemtable::with_capacity(1024);
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
