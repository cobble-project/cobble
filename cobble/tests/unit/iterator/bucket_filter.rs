use super::*;
use crate::iterator::mock_iterator::MockIterator;
use crate::r#type::KvValue;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

struct CountingIterator {
    inner: MockIterator,
    seek_targets: Arc<Mutex<Vec<Vec<u8>>>>,
    seek_to_first_calls: Arc<AtomicUsize>,
    next_calls: Arc<AtomicUsize>,
}

impl<'a> KvIterator<'a> for CountingIterator {
    fn seek(&mut self, target: &[u8]) -> Result<()> {
        self.seek_targets.lock().unwrap().push(target.to_vec());
        self.inner.seek(target)
    }

    fn seek_to_first(&mut self) -> Result<()> {
        self.seek_to_first_calls.fetch_add(1, Ordering::Relaxed);
        self.inner.seek_to_first()
    }

    fn next(&mut self) -> Result<bool> {
        self.next_calls.fetch_add(1, Ordering::Relaxed);
        self.inner.next()
    }

    fn valid(&self) -> bool {
        self.inner.valid()
    }

    fn key(&self) -> Result<Option<&[u8]>> {
        self.inner.key()
    }

    fn take_key(&mut self) -> Result<Option<Bytes>> {
        self.inner.take_key()
    }

    fn take_value(&mut self) -> Result<Option<KvValue>> {
        self.inner.take_value()
    }
}

fn key(bucket: u16, suffix: u8) -> Vec<u8> {
    let mut key = encode_bucket_prefix(bucket).to_vec();
    key.push(suffix);
    key
}

fn entries(buckets: &[u16]) -> Vec<(Vec<u8>, Vec<u8>)> {
    buckets
        .iter()
        .map(|bucket| (key(*bucket, 0), bucket.to_be_bytes().to_vec()))
        .collect()
}

fn collect_buckets(iter: &mut BucketFilterIterator<MockIterator>) -> Result<Vec<u16>> {
    let mut buckets = Vec::new();
    while iter.valid() {
        let key = iter.key()?.expect("valid iterator must have a key");
        buckets.push(u16::from_be_bytes([key[0], key[1]]));
        if !iter.next()? {
            break;
        }
    }
    Ok(buckets)
}

#[test]
fn seek_to_first_uses_numeric_bucket_order_across_byte_boundary() {
    let inner = MockIterator::new(entries(&[254, 255, 256, 257]));
    let mut iter = BucketFilterIterator::new(inner, 255..=256);

    iter.seek_to_first().unwrap();

    assert_eq!(collect_buckets(&mut iter).unwrap(), vec![255, 256]);
    assert!(!iter.valid());
    assert!(iter.key().unwrap().is_none());
}

#[test]
fn seek_clamps_targets_below_range_and_preserves_targets_inside_range() {
    let entries = vec![
        (key(9, 0), vec![9]),
        (key(10, 0), vec![10]),
        (key(10, 2), vec![12]),
        (key(11, 0), vec![11]),
        (key(12, 0), vec![12]),
    ];
    let mut iter = BucketFilterIterator::new(MockIterator::new(entries), 10..=11);

    iter.seek(&key(9, 1)).unwrap();
    assert_eq!(iter.key().unwrap(), Some(key(10, 0).as_slice()));

    iter.seek(&key(10, 1)).unwrap();
    assert_eq!(iter.key().unwrap(), Some(key(10, 2).as_slice()));
}

#[test]
fn seek_at_or_above_end_exhausts_without_exposing_inner_key() {
    let inner = MockIterator::new(entries(&[10, 11, 12]));
    let mut iter = BucketFilterIterator::new(inner, 10..=11);

    iter.seek(&key(12, 0)).unwrap();

    assert!(!iter.valid());
    assert!(iter.key().unwrap().is_none());
    assert!(iter.take_key().unwrap().is_none());
    assert!(iter.take_value().unwrap().is_none());
    assert!(!iter.next().unwrap());
}

#[test]
fn maximum_bucket_has_no_finite_upper_bound() {
    let inner = MockIterator::new(entries(&[u16::MAX - 1, u16::MAX]));
    let mut iter = BucketFilterIterator::new(inner, u16::MAX..=u16::MAX);

    iter.seek_to_first().unwrap();

    assert_eq!(collect_buckets(&mut iter).unwrap(), vec![u16::MAX]);
}

#[test]
fn value_is_available_inside_range() {
    let inner = MockIterator::new(entries(&[20]));
    let mut iter = BucketFilterIterator::new(inner, 20..=20);

    iter.seek_to_first().unwrap();

    let Some(KvValue::Encoded(value)) = iter.take_value().unwrap() else {
        panic!("expected encoded value");
    };
    assert_eq!(value.as_ref(), &20u16.to_be_bytes());
}

#[test]
fn narrow_range_seeks_instead_of_scanning_excluded_buckets() {
    let seek_targets = Arc::new(Mutex::new(Vec::new()));
    let seek_to_first_calls = Arc::new(AtomicUsize::new(0));
    let next_calls = Arc::new(AtomicUsize::new(0));
    let inner = CountingIterator {
        inner: MockIterator::new(entries(&(0..=1000).collect::<Vec<_>>())),
        seek_targets: Arc::clone(&seek_targets),
        seek_to_first_calls: Arc::clone(&seek_to_first_calls),
        next_calls: Arc::clone(&next_calls),
    };
    let mut iter = BucketFilterIterator::new(inner, 900..=901);

    iter.seek_to_first().unwrap();
    let mut buckets = Vec::new();
    while iter.valid() {
        let key = iter.key().unwrap().unwrap();
        buckets.push(u16::from_be_bytes([key[0], key[1]]));
        if !iter.next().unwrap() {
            break;
        }
    }

    assert_eq!(buckets, vec![900, 901]);
    assert_eq!(
        seek_targets.lock().unwrap().as_slice(),
        &[encode_bucket_prefix(900).to_vec()]
    );
    assert_eq!(seek_to_first_calls.load(Ordering::Relaxed), 0);
    assert_eq!(next_calls.load(Ordering::Relaxed), 2);
}
