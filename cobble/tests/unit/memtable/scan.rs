//! Online memtable scan coverage for visibility, entry ownership, and rotation.

use super::super::{ActiveMemtableContents, tests::build_test_memtable};
use super::*;
use crate::config::MemtableType;
use crate::key_codec::encode_scan_key;
use crate::memtable::MemtableReclaimer;
use crate::sst::row_codec::decode_value;
use crate::r#type::{RefColumn, RefKey, RefValue};
use std::ops::ControlFlow;
use uuid::Uuid;

#[test]
fn active_scan_seals_entries_appended_after_iterator_creation() {
    for memtable_type in [
        MemtableType::Hash,
        MemtableType::Skiplist,
        MemtableType::Vec,
    ] {
        let schema = Arc::new(Schema::new(1, 1, Vec::new()));
        let active = Arc::new(RwLock::new(ActiveMemtable {
            id: Uuid::new_v4(),
            schema,
            sealed_data_end: 0,
            contents: ActiveMemtableContents::Writable(build_test_memtable(memtable_type, &[])),
        }));
        let mut iter = MemtableScanIterator::for_active(
            Arc::clone(&active),
            None,
            None,
            None,
            None,
            None,
            false,
        );

        let key = RefKey::new(0, b"key");
        let old = RefValue::new(vec![Some(RefColumn::new(ValueType::Put, b"old"))]);
        let new = RefValue::new(vec![Some(RefColumn::new(ValueType::Put, b"new"))]);
        {
            let mut active = active.write().unwrap();
            active
                .writable_memtable()
                .unwrap()
                .put_ref(&key, &old, 1)
                .unwrap();
        }

        iter.seek_to_first().unwrap();
        let captured_end = active
            .read()
            .unwrap()
            .readable_memtable()
            .unwrap()
            .data_offset();
        {
            let mut active = active.write().unwrap();
            assert_eq!(active.sealed_data_end, captured_end, "{memtable_type:?}");
            active.put_ref_or_replace(&key, &new, 1).unwrap();
            assert!(
                active.readable_memtable().unwrap().data_offset() > captured_end,
                "{memtable_type:?}"
            );
        }

        let mut retained = iter.take_value().unwrap().unwrap().unwrap_encoded();
        let retained = decode_value(&mut retained, 1).unwrap();
        assert_eq!(
            retained.columns()[0].as_ref().unwrap().data().as_ref(),
            b"old",
            "{memtable_type:?}"
        );
    }
}

#[test]
fn concurrent_combined_skiplist_writes_preserve_keys_and_retained_snapshot_bytes() {
    let active = Arc::new(RwLock::new(ActiveMemtable {
        id: Uuid::new_v4(),
        schema: Arc::new(Schema::new(1, 1, Vec::new())),
        sealed_data_end: 0,
        contents: ActiveMemtableContents::Writable(MemtableImpl::Skiplist(
            SkiplistMemtable::with_capacity(128 * 1024),
        )),
    }));
    let key = RefKey::new(0, b"shared");
    let old = RefValue::new(vec![Some(RefColumn::new(ValueType::Put, b"old"))]);
    active
        .write()
        .unwrap()
        .put_ref_or_replace(&key, &old, 1)
        .unwrap();
    let mut snapshot =
        MemtableScanIterator::for_active(Arc::clone(&active), None, None, None, None, None, false);
    snapshot.seek_to_first().unwrap();
    let mut retained = snapshot.take_value().unwrap().unwrap().unwrap_encoded();
    let barrier = Arc::new(std::sync::Barrier::new(4));
    let mut writers = Vec::new();
    for worker in 0..4 {
        let active = Arc::clone(&active);
        let barrier = Arc::clone(&barrier);
        writers.push(std::thread::spawn(move || {
            let data = [worker as u8; 3];
            let value = RefValue::new(vec![Some(RefColumn::new(ValueType::Put, &data))]);
            barrier.wait();
            for index in 0..128 {
                let key_bytes = format!("key-{worker}-{index:03}");
                let mut guard = active.write().unwrap();
                guard
                    .put_ref_or_replace(&RefKey::new(0, key_bytes.as_bytes()), &value, 1)
                    .unwrap();
                guard
                    .put_ref_or_replace(&RefKey::new(0, b"shared"), &value, 1)
                    .unwrap();
            }
        }));
    }
    for writer in writers {
        writer.join().unwrap();
    }
    let retained = decode_value(&mut retained, 1).unwrap();
    assert_eq!(
        retained.columns()[0].as_ref().unwrap().data().as_ref(),
        b"old"
    );
    let guard = active.read().unwrap();
    let memtable = guard.readable_memtable().unwrap();
    for worker in 0..4 {
        for index in 0..128 {
            let key_bytes = format!("key-{worker}-{index:03}");
            let encoded_key = encode_scan_key(0, 0, key_bytes.as_bytes());
            let mut encoded_value = Bytes::copy_from_slice(memtable.get(&encoded_key).unwrap());
            let value = decode_value(&mut encoded_value, 1).unwrap();
            assert_eq!(
                value.columns()[0].as_ref().unwrap().data().as_ref(),
                [worker as u8; 3]
            );
        }
    }
    let shared_key = encode_scan_key(0, 0, b"shared");
    let mut versions = 0;
    assert!(
        memtable
            .visit_values_until(&shared_key, |_| {
                versions += 1;
                Ok(ControlFlow::Continue(()))
            })
            .unwrap()
            .is_continue()
    );
    assert_eq!(versions, 2);
    let mut iter = memtable.iter();
    iter.seek_to_first().unwrap();
    let mut count = 0;
    while iter.next().unwrap() {
        count += 1;
    }
    assert_eq!(count, 4 * 128 + 2);
}

#[test]
fn unlimited_collection_defers_value_decode_and_does_not_update_shadow_allowance() {
    let filter = MemtableRowFilter {
        num_columns: 1,
        selected_columns: Some(vec![0]),
    };
    for memtable_type in [
        MemtableType::Hash,
        MemtableType::Skiplist,
        MemtableType::Vec,
    ] {
        let mut memtable = build_test_memtable(memtable_type, &[]);
        let key = encode_scan_key(0, 0, b"key");
        memtable.put(&key, b"").unwrap();
        let allowance = Arc::new(AtomicUsize::new(3));
        let entries = collect_limited_entries_from_memtable(
            &memtable,
            None,
            None,
            Some(&filter),
            None,
            Some(&allowance),
        )
        .unwrap();
        assert_eq!(entries, vec![(key, Bytes::new())]);
        assert_eq!(allowance.load(Ordering::Relaxed), 3);
        // Collection defers decoding rather than hiding corruption from the normal read path.
        assert!(
            KvValue::encoded(entries[0].1.clone())
                .into_decoded(1)
                .is_err()
        );
        assert!(
            collect_limited_entries_from_memtable(
                &memtable,
                None,
                None,
                Some(&filter),
                Some(1),
                Some(&allowance),
            )
            .is_err(),
            "a limited scan still evaluates visibility"
        );
    }
}

fn build_test_skiplist_with_reclaimer(
    entries: &[(&[u8], ValueType, &[u8])],
    reclaimer: MemtableReclaimer,
) -> Arc<MemtableImpl> {
    let mut memtable = SkiplistMemtable::with_buffer_and_reclaimer(vec![0; 4096], reclaimer);
    for (key_bytes, value_type, value_bytes) in entries {
        let key = RefKey::new(0, key_bytes);
        let value = RefValue::new(vec![Some(RefColumn::new(*value_type, value_bytes))]);
        memtable.put_ref(&key, &value, 1).unwrap();
    }
    Arc::new(MemtableImpl::Skiplist(memtable))
}

#[test]
fn skiplist_scan_cursor_immutable_pins_and_borrows_without_copy() {
    let reclaimed = Arc::new(AtomicUsize::new(0));
    let reclaimer: MemtableReclaimer = {
        let reclaimed = Arc::clone(&reclaimed);
        Arc::new(move |_| {
            reclaimed.fetch_add(1, Ordering::Relaxed);
        })
    };
    let memtable = build_test_skiplist_with_reclaimer(
        &[
            (b"a", ValueType::Put, b"outside"),
            (b"b", ValueType::Put, b"old"),
            (b"b", ValueType::Delete, b""),
            (b"b", ValueType::Put, b"new"),
            (b"c", ValueType::Put, b"outside"),
        ],
        reclaimer,
    );
    let start = encode_scan_key(0, 0, b"b");
    let end = encode_scan_key(0, 0, b"c");
    let expected_key_ptr = match memtable.as_ref() {
        MemtableImpl::Skiplist(skiplist_memtable) => {
            let mut iter = skiplist_memtable.iter();
            iter.seek(start.as_ref()).unwrap();
            assert!(iter.next().unwrap());
            iter.key().unwrap().unwrap().as_ptr()
        }
        _ => unreachable!(),
    };
    let mut cursor = SkiplistScanCursor::new(
        MemtableScanSource::Immutable(Arc::clone(&memtable)),
        Some(end),
    );
    drop(memtable);
    assert_eq!(reclaimed.load(Ordering::Relaxed), 0);

    cursor.seek(start.as_ref()).unwrap();
    let retained_key = cursor.take_key().unwrap().unwrap();
    let retained_value = cursor.take_value().unwrap().unwrap().unwrap_encoded();
    assert_eq!(retained_key.as_ptr(), expected_key_ptr);
    assert_eq!(
        retained_value.as_ptr(),
        retained_key.as_ptr().wrapping_add(retained_key.len())
    );
    assert_eq!(retained_key.as_ref(), start.as_ref());
    assert!(cursor.next().unwrap());
    for (index, (expected_type, expected_data)) in [
        (ValueType::Delete, b"".as_slice()),
        (ValueType::Put, b"old".as_slice()),
    ]
    .into_iter()
    .enumerate()
    {
        assert!(cursor.valid());
        assert_eq!(cursor.key().unwrap(), Some(start.as_ref()));
        let mut value = cursor.take_value().unwrap().unwrap().unwrap_encoded();
        let decoded = decode_value(&mut value, 1).unwrap();
        let column = decoded.columns()[0].as_ref().unwrap();
        assert_eq!(column.value_type, expected_type);
        assert_eq!(column.data().as_ref(), expected_data);
        assert_eq!(cursor.next().unwrap(), index == 0);
    }
    assert!(!cursor.valid());
    drop(cursor);
    assert_eq!(reclaimed.load(Ordering::Relaxed), 0);
    let mut retained_value = retained_value;
    let retained = decode_value(&mut retained_value, 1).unwrap();
    assert_eq!(
        retained.columns()[0].as_ref().unwrap().data().as_ref(),
        b"new"
    );
    drop(retained);
    drop(retained_key);
    drop(retained_value);
    assert_eq!(reclaimed.load(Ordering::Relaxed), 1);
}

#[test]
fn skiplist_scan_cursor_active_releases_lock_and_retains_entry_across_rotation() {
    let schema = Arc::new(Schema::new(1, 1, Vec::new()));
    let active = Arc::new(RwLock::new(ActiveMemtable {
        id: Uuid::new_v4(),
        schema,
        sealed_data_end: 0,
        contents: ActiveMemtableContents::Writable(build_test_memtable(
            MemtableType::Skiplist,
            &[
                (b"a", ValueType::Put, b"value-a"),
                (b"b", ValueType::Put, b"value-b"),
            ],
        )),
    }));
    let mut active_cursor =
        SkiplistScanCursor::new(MemtableScanSource::Active(Arc::clone(&active)), None);

    active_cursor.seek_to_first().unwrap();
    assert!(active.try_write().is_ok());

    let rotated = {
        let mut active_guard = active.write().unwrap();
        active_guard.freeze().unwrap()
    };
    let key = active_cursor.take_key().unwrap().unwrap();
    let value = active_cursor
        .take_value()
        .unwrap()
        .unwrap()
        .unwrap_encoded();
    drop(rotated);
    assert_eq!(key.as_ref(), encode_scan_key(0, 0, b"a").as_ref());
    let mut decoded = value;
    assert_eq!(
        decode_value(&mut decoded, 1).unwrap().columns()[0]
            .as_ref()
            .unwrap()
            .data()
            .as_ref(),
        b"value-a"
    );
    assert!(active_cursor.next().unwrap());
    assert_eq!(
        active_cursor.key().unwrap(),
        Some(encode_scan_key(0, 0, b"b").as_ref())
    );
    let mut value = active_cursor
        .take_value()
        .unwrap()
        .unwrap()
        .unwrap_encoded();
    assert_eq!(
        decode_value(&mut value, 1).unwrap().columns()[0]
            .as_ref()
            .unwrap()
            .data()
            .as_ref(),
        b"value-b"
    );
}
