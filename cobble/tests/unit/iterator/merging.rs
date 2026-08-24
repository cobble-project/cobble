use super::*;
use crate::iterator::mock_iterator::MockIterator;
use bytes::Bytes;

struct BoundaryMockIterator {
    entries: Vec<(Bytes, Bytes)>,
    index: usize,
    pause_after_index: Option<usize>,
    should_stop_at_block_boundary: bool,
    pending_resume: bool,
    remaining_resume_boundaries: usize,
    stopped_at_block_boundary: bool,
}

impl BoundaryMockIterator {
    fn new<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        entries: Vec<(K, V)>,
        pause_after_index: Option<usize>,
    ) -> Self {
        Self {
            entries: entries
                .into_iter()
                .map(|(key, value)| {
                    (
                        Bytes::copy_from_slice(key.as_ref()),
                        Bytes::copy_from_slice(value.as_ref()),
                    )
                })
                .collect(),
            index: usize::MAX,
            pause_after_index,
            should_stop_at_block_boundary: false,
            pending_resume: false,
            remaining_resume_boundaries: 0,
            stopped_at_block_boundary: false,
        }
    }

    fn with_resume_boundaries(mut self, count: usize) -> Self {
        self.remaining_resume_boundaries = count;
        self
    }
}

impl<'a> KvIterator<'a> for BoundaryMockIterator {
    fn seek(&mut self, target: &[u8]) -> Result<()> {
        self.pending_resume = false;
        self.stopped_at_block_boundary = false;
        self.index = self
            .entries
            .iter()
            .position(|(key, _)| key.as_ref() >= target)
            .unwrap_or(self.entries.len());
        Ok(())
    }

    fn seek_to_first(&mut self) -> Result<()> {
        self.pending_resume = false;
        self.stopped_at_block_boundary = false;
        self.index = 0;
        Ok(())
    }

    fn next(&mut self) -> Result<bool> {
        if self.stopped_at_block_boundary {
            return Ok(false);
        }
        self.stopped_at_block_boundary = false;
        if self.pending_resume {
            if self.should_stop_at_block_boundary && self.remaining_resume_boundaries > 0 {
                self.remaining_resume_boundaries -= 1;
                self.stopped_at_block_boundary = true;
                return Ok(false);
            }
            self.pending_resume = false;
            self.index += 1;
            return Ok(self.index < self.entries.len());
        }
        if self.index >= self.entries.len() {
            return Ok(false);
        }
        if self.should_stop_at_block_boundary && self.pause_after_index == Some(self.index) {
            self.pending_resume = true;
            self.stopped_at_block_boundary = true;
            return Ok(false);
        }
        self.index += 1;
        Ok(self.index < self.entries.len())
    }

    fn valid(&self) -> bool {
        !self.stopped_at_block_boundary && self.index < self.entries.len()
    }

    fn key(&self) -> Result<Option<&[u8]>> {
        Ok(self.entries.get(self.index).map(|(key, _)| key.as_ref()))
    }

    fn take_key(&mut self) -> Result<Option<Bytes>> {
        Ok(self.entries.get(self.index).map(|(key, _)| key.clone()))
    }

    fn take_value(&mut self) -> Result<Option<KvValue>> {
        Ok(self
            .entries
            .get(self.index)
            .map(|(_, value)| KvValue::Encoded(value.clone())))
    }

    fn set_stop_at_block_boundary(&mut self, enabled: bool) {
        self.should_stop_at_block_boundary = enabled;
        self.pending_resume = false;
        self.stopped_at_block_boundary = false;
    }

    fn clear_stop_at_block_boundary(&mut self) {
        self.stopped_at_block_boundary = false;
    }

    fn stopped_at_block_boundary(&self) -> bool {
        self.stopped_at_block_boundary
    }
}

#[test]
fn test_merging_iterator_basic() {
    let iter1 = MockIterator::new(vec![(b"a".as_slice(), b"1"), (b"c", b"3"), (b"e", b"5")]);
    let iter2 = MockIterator::new(vec![(b"b".as_slice(), b"2"), (b"d", b"4"), (b"f", b"6")]);

    let mut merger = MergingIterator::new(vec![iter1, iter2]);
    merger.seek_to_first().unwrap();

    let mut results = vec![];
    while merger.valid() {
        let (k, kv) = merger.take_current().unwrap().unwrap();
        let v = kv.unwrap_encoded();
        results.push((k, v));
        merger.next().unwrap();
    }

    assert_eq!(results.len(), 6);
    assert_eq!(results[0].0.as_ref(), b"a");
    assert_eq!(results[1].0.as_ref(), b"b");
    assert_eq!(results[2].0.as_ref(), b"c");
    assert_eq!(results[3].0.as_ref(), b"d");
    assert_eq!(results[4].0.as_ref(), b"e");
    assert_eq!(results[5].0.as_ref(), b"f");
}

#[test]
fn test_merging_iterator_overlapping_keys() {
    // Test with duplicate keys across iterators
    let iter1 = MockIterator::new(vec![
        (b"a".as_slice(), b"v1-a"),
        (b"b", b"v1-b"),
        (b"c", b"v1-c"),
    ]);
    let iter2 = MockIterator::new(vec![
        (b"a".as_slice(), b"v2-a"),
        (b"b", b"v2-b"),
        (b"d", b"v2-d"),
    ]);

    let mut merger = MergingIterator::new(vec![iter1, iter2]);
    merger.seek_to_first().unwrap();

    let mut results = vec![];
    while merger.valid() {
        let (k, kv) = merger.take_current().unwrap().unwrap();
        let v = kv.unwrap_encoded();
        results.push((k, v));
        merger.next().unwrap();
    }

    // All entries should be present, including duplicates
    assert_eq!(results.len(), 6);
    assert_eq!(results[0].0.as_ref(), b"a");
    assert_eq!(results[0].1.as_ref(), b"v1-a"); // First iterator wins for same key
    assert_eq!(results[1].0.as_ref(), b"a");
    assert_eq!(results[1].1.as_ref(), b"v2-a");
    assert_eq!(results[2].0.as_ref(), b"b");
    assert_eq!(results[3].0.as_ref(), b"b");
    assert_eq!(results[4].0.as_ref(), b"c");
    assert_eq!(results[5].0.as_ref(), b"d");
}

#[test]
fn test_merging_iterator_seek() {
    let iter1 = MockIterator::new(vec![(b"a".as_slice(), b"1"), (b"c", b"3"), (b"e", b"5")]);
    let iter2 = MockIterator::new(vec![(b"b".as_slice(), b"2"), (b"d", b"4"), (b"f", b"6")]);

    let mut merger = MergingIterator::new(vec![iter1, iter2]);

    // Seek to "c"
    merger.seek(b"c").unwrap();
    assert!(merger.valid());
    assert_eq!(merger.key().unwrap().unwrap(), b"c");

    // Seek to "d"
    merger.seek(b"d").unwrap();
    assert!(merger.valid());
    assert_eq!(merger.key().unwrap().unwrap(), b"d");

    // Seek to non-existent key between entries
    merger.seek(b"ca").unwrap();
    assert!(merger.valid());
    assert_eq!(merger.key().unwrap().unwrap(), b"d");
}

#[test]
fn test_merging_iterator_empty() {
    let iter1 = MockIterator::new(Vec::<(&[u8], &[u8])>::new());
    let iter2 = MockIterator::new(Vec::<(&[u8], &[u8])>::new());

    let mut merger = MergingIterator::new(vec![iter1, iter2]);
    merger.seek_to_first().unwrap();

    assert!(!merger.valid());
    assert!(merger.take_current().unwrap().is_none());
}

#[test]
fn test_merging_iterator_single() {
    let iter1 = MockIterator::new(vec![(b"a".as_slice(), b"1"), (b"b", b"2"), (b"c", b"3")]);

    let mut merger = MergingIterator::new(vec![iter1]);
    merger.seek_to_first().unwrap();

    let mut count = 0;
    while merger.valid() {
        count += 1;
        merger.next().unwrap();
    }

    assert_eq!(count, 3);
}

#[test]
fn test_merging_iterator_many() {
    let iter1 = MockIterator::new(vec![(b"a".as_slice(), b"1"), (b"d", b"4")]);
    let iter2 = MockIterator::new(vec![(b"b".as_slice(), b"2"), (b"e", b"5")]);
    let iter3 = MockIterator::new(vec![(b"c".as_slice(), b"3"), (b"f", b"6")]);

    let mut merger = MergingIterator::new(vec![iter1, iter2, iter3]);
    merger.seek_to_first().unwrap();

    let mut results = vec![];
    while merger.valid() {
        let (k, _) = merger.take_current().unwrap().unwrap();
        results.push(k);
        merger.next().unwrap();
    }

    assert_eq!(results.len(), 6);
    assert_eq!(results[0].as_ref(), b"a");
    assert_eq!(results[1].as_ref(), b"b");
    assert_eq!(results[2].as_ref(), b"c");
    assert_eq!(results[3].as_ref(), b"d");
    assert_eq!(results[4].as_ref(), b"e");
    assert_eq!(results[5].as_ref(), b"f");
}

#[test]
fn test_merging_iterator_resumes_after_child_boundary_stop() {
    let iter1 = BoundaryMockIterator::new(vec![(b"a".as_slice(), b"1"), (b"e", b"5")], Some(0))
        .with_resume_boundaries(1);
    let iter2 = BoundaryMockIterator::new(vec![(b"b".as_slice(), b"2"), (b"c", b"3")], None);
    let mut merger = MergingIterator::new(vec![iter1, iter2]);
    merger.set_stop_at_block_boundary(true);
    merger.seek_to_first().unwrap();

    let mut keys: Vec<Bytes> = vec![merger.take_current().unwrap().unwrap().0];
    let mut boundary_keys = Vec::new();
    loop {
        if merger.next().unwrap() {
            keys.push(merger.take_current().unwrap().unwrap().0);
            continue;
        }
        if merger.stopped_at_block_boundary() {
            boundary_keys.push(keys.last().cloned().unwrap());
            merger.clear_stop_at_block_boundary();
            continue;
        }
        break;
    }

    assert_eq!(
        keys,
        vec![
            Bytes::from_static(b"a"),
            Bytes::from_static(b"b"),
            Bytes::from_static(b"c"),
            Bytes::from_static(b"e"),
        ]
    );
    assert_eq!(
        boundary_keys,
        vec![Bytes::from_static(b"a"), Bytes::from_static(b"a")]
    );
}

#[test]
fn test_merging_iterator_one_empty() {
    let iter1 = MockIterator::new(vec![(b"a".as_slice(), b"1".as_slice()), (b"c", b"3")]);
    let iter2 = MockIterator::new(Vec::<(&[u8], &[u8])>::new());
    let iter3 = MockIterator::new(vec![(b"b".as_slice(), b"2".as_slice()), (b"d", b"4")]);

    let mut merger = MergingIterator::new(vec![iter1, iter2, iter3]);
    merger.seek_to_first().unwrap();

    let mut results = vec![];
    while merger.valid() {
        let (k, _) = merger.take_current().unwrap().unwrap();
        results.push(k);
        merger.next().unwrap();
    }

    assert_eq!(results.len(), 4);
    assert_eq!(results[0].as_ref(), b"a");
    assert_eq!(results[1].as_ref(), b"b");
    assert_eq!(results[2].as_ref(), b"c");
    assert_eq!(results[3].as_ref(), b"d");
}
