use super::*;
use crate::iterator::KvIterator;
use crate::r#type::{RefColumn, ValueType};

#[test]
fn inplace_replace_latest_requires_equal_length_and_unsealed_entry() {
    let mut mem = HashMemtable::with_capacity(1024);
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

#[test]
fn put_and_get() {
    let mut mem = HashMemtable::with_capacity(1024);
    mem.put(b"key1", b"value1").unwrap();
    mem.put(b"key2", b"value2").unwrap();

    assert_eq!(mem.get(b"key1").unwrap(), b"value1");
    assert_eq!(mem.get(b"key2").unwrap(), b"value2");
    assert!(mem.get(b"missing").is_none());
}

#[test]
fn overwrite_updates_value() {
    let mut mem = HashMemtable::with_capacity(1024);
    mem.put(b"key", b"old").unwrap();
    mem.put(b"key", b"new").unwrap();
    assert_eq!(mem.get(b"key").unwrap(), b"new");
}

#[test]
fn capacity_enforced() {
    let mut mem = HashMemtable::with_capacity(64);
    mem.put(b"k1", b"v1").unwrap();
    let err = mem.put(b"k2", b"value_too_long").unwrap_err();
    match err {
        Error::MemtableFull { .. } => {}
        _ => panic!("unexpected error type"),
    }
}

#[test]
fn remaining_capacity_updates() {
    let mut mem = HashMemtable::with_capacity(100);
    let before = mem.remaining_capacity();
    mem.put(b"k", b"v").unwrap();
    assert!(mem.remaining_capacity() < before);
}

#[test]
fn bucket_distribution_and_lookup() {
    // Use small bucket count to force chaining.
    let mut mem = HashMemtable::with_capacity_and_buckets(256, 4);
    mem.put(b"key1", b"v1").unwrap();
    mem.put(b"key2", b"v2").unwrap();
    mem.put(b"key3", b"v3").unwrap();

    assert_eq!(mem.get(b"key1").unwrap(), b"v1");
    assert_eq!(mem.get(b"key2").unwrap(), b"v2");
    assert_eq!(mem.get(b"key3").unwrap(), b"v3");
}

#[test]
fn entry_stats_count_successful_writes_including_duplicate_keys() {
    let mut mem = HashMemtable::with_capacity_and_buckets(128, 1);
    assert_eq!(mem.entry_count(), 0);
    assert_eq!(mem.used_entry_bytes(), 0);

    mem.put(b"key", b"value").unwrap();
    let entry_bytes = HashMemtable::entry_size(3, 5) + HashMemtable::index_entry_size();
    assert_eq!(mem.entry_count(), 1);
    assert_eq!(mem.used_entry_bytes(), entry_bytes);

    mem.put(b"key", b"value").unwrap();
    assert_eq!(mem.entry_count(), 2);
    assert_eq!(mem.used_entry_bytes(), entry_bytes * 2);

    let mut full_mem = HashMemtable::with_capacity_and_buckets(64, 1);
    full_mem.put(b"k", b"v").unwrap();
    let entry_count = full_mem.entry_count();
    let used_entry_bytes = full_mem.used_entry_bytes();
    assert!(matches!(
        full_mem.put(b"k", &[b'x'; 32]),
        Err(Error::MemtableFull { .. })
    ));
    assert_eq!(full_mem.entry_count(), entry_count);
    assert_eq!(full_mem.used_entry_bytes(), used_entry_bytes);
}

#[test]
fn bucket_count_scales_with_memtable_capacity() {
    assert_eq!(
        HashMemtable::default_bucket_count(128 * 1024 * 1024),
        1024 * 1024
    );
}

#[test]
fn get_all_returns_latest_first() {
    let mut mem = HashMemtable::with_capacity(512);
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
    let mut mem = HashMemtable::with_capacity(1024);
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
