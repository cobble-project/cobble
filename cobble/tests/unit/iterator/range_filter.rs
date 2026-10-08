use super::*;
use crate::iterator::SchemaTaggedIterator;
use crate::iterator::mock_iterator::CountingMockIterator;
use std::sync::atomic::Ordering;

#[test]
fn encoded_bounds_filter_versions_without_reading_excluded_values_and_seek_resets_end() {
    for (lower, end, expected) in [
        (None, None, vec!["", "a", "a", "c", "c", "e"]),
        (None, Some(("", false)), vec![]),
        (None, Some(("", true)), vec![""]),
        (None, Some(("c", false)), vec!["", "a", "a"]),
        (None, Some(("c", true)), vec!["", "a", "a", "c", "c"]),
        (Some("a"), Some(("c", true)), vec!["c", "c"]),
        (Some("c"), Some(("c", true)), vec![]),
        (Some("e"), Some(("c", false)), vec![]),
    ] {
        let (input, counts) = CountingMockIterator::new(
            ["", "a", "a", "c", "c", "e"]
                .into_iter()
                .map(|key| (key, "value"))
                .collect(),
        );
        let mut iter = RangeFilterIterator::new(
            SchemaTaggedIterator::new(input, 42),
            lower.map(Bytes::from),
            end.map(|(key, inclusive)| (Bytes::from(key), inclusive)),
        );
        iter.seek_to_first().unwrap();
        let mut actual = Vec::new();
        while iter.valid() {
            assert_eq!(iter.current_schema_id(), Some(42));
            let (key, _) = iter.take_current().unwrap().unwrap();
            actual.push(key);
            iter.next().unwrap();
        }
        let expected: Vec<_> = expected.into_iter().map(Bytes::from).collect();
        assert_eq!(actual, expected, "lower={lower:?}, end={end:?}");
        assert_eq!(*counts.value_keys.lock().unwrap(), expected);
        assert!(iter.key().unwrap().is_none());
        assert!(iter.take_key().unwrap().is_none());
        assert!(iter.take_value().unwrap().is_none());
        assert!(iter.take_current().unwrap().is_none());
        assert_eq!(iter.current_schema_id(), None);
        if iter.end_reached {
            let next_calls = counts.next.load(Ordering::Relaxed);
            iter.clear_stop_at_block_boundary();
            for _ in 0..3 {
                assert!(!iter.next().unwrap());
            }
            assert_eq!(counts.next.load(Ordering::Relaxed), next_calls);
            assert!(!iter.stopped_at_block_boundary());
        }
        iter.seek(b"a").unwrap();
        assert_eq!(
            iter.valid(),
            expected.iter().any(|key| key.as_ref() >= b"a".as_slice())
        );
        iter.seek(b"z").unwrap();
        assert!(!iter.valid());
        iter.seek_to_first().unwrap();
        assert_eq!(
            iter.key().unwrap(),
            expected.first().map(|key| key.as_ref())
        );
    }
}

#[test]
fn block_pause_resumes_lower_filtering_but_upper_end_cannot_resume() {
    for (lower, end, expected) in [
        (Some("a"), ("c", true), Some("c")),
        (None, ("a", true), None),
        (None, ("c", false), None),
    ] {
        let (input, counts) = CountingMockIterator::new(vec![("a", "a"), ("c", "c"), ("e", "e")]);
        let mut iter = RangeFilterIterator::new(
            input.with_pause_after_index(0),
            lower.map(Bytes::from),
            Some((Bytes::from(end.0), end.1)),
        );
        iter.set_stop_at_block_boundary(true);
        iter.seek_to_first().unwrap();
        if lower.is_none() {
            assert_eq!(iter.key().unwrap(), Some(b"a".as_slice()));
            assert!(!iter.next().unwrap());
        }
        assert!(!iter.valid());
        assert!(iter.stopped_at_block_boundary());
        assert!(!iter.end_reached);
        assert!(iter.take_value().unwrap().is_none());
        iter.clear_stop_at_block_boundary();
        assert_eq!(iter.next().unwrap(), expected.is_some());
        assert_eq!(iter.key().unwrap(), expected.map(str::as_bytes));
        if iter.valid() {
            iter.take_current().unwrap().unwrap();
            assert!(!iter.next().unwrap());
        }
        assert!(iter.end_reached);
        let next_calls = counts.next.load(Ordering::Relaxed);
        iter.clear_stop_at_block_boundary();
        assert!(!iter.next().unwrap());
        assert_eq!(counts.next.load(Ordering::Relaxed), next_calls);
        assert!(!iter.stopped_at_block_boundary());
    }
}
