use super::*;
use crate::data_file::DataFileType;
use crate::iterator::mock_iterator::MockIterator;
use std::sync::atomic::{AtomicUsize, Ordering};

struct CountingIterator {
    inner: MockIterator,
    seek_calls: Arc<AtomicUsize>,
    seek_to_first_calls: Arc<AtomicUsize>,
}

impl<'a> KvIterator<'a> for CountingIterator {
    fn seek(&mut self, target: &[u8]) -> Result<()> {
        self.seek_calls.fetch_add(1, Ordering::Relaxed);
        self.inner.seek(target)
    }

    fn seek_to_first(&mut self) -> Result<()> {
        self.seek_to_first_calls.fetch_add(1, Ordering::Relaxed);
        self.inner.seek_to_first()
    }

    fn next(&mut self) -> Result<bool> {
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

fn create_data_file(id: u64, start: &[u8], end: &[u8]) -> Arc<DataFile> {
    let bucket_range = DataFile::bucket_range_from_keys(start, end);
    Arc::new(DataFile::new_untracked(
        DataFileType::SSTable,
        start.to_vec(),
        end.to_vec(),
        id,
        0,
        0,
        bucket_range.clone(),
        bucket_range,
    ))
}

#[test]
fn test_sorted_run_basic() {
    let files = vec![
        create_data_file(1, b"a", b"c"),
        create_data_file(2, b"d", b"f"),
        create_data_file(3, b"g", b"i"),
    ];

    let run = SortedRun::new(1, files);
    assert_eq!(run.len(), 3);
    assert!(!run.is_empty());
    assert_eq!(run.start_key(), Some(b"a".as_slice()));
    assert_eq!(run.end_key(), Some(b"i".as_slice()));
}

#[test]
fn test_sorted_run_empty() {
    let run = SortedRun::new(1, vec![]);
    assert_eq!(run.len(), 0);
    assert!(run.is_empty());
    assert_eq!(run.start_key(), None);
    assert_eq!(run.end_key(), None);
}

#[test]
fn test_find_file() {
    let files = vec![
        create_data_file(1, b"a", b"c"),
        create_data_file(2, b"d", b"f"),
        create_data_file(3, b"g", b"i"),
    ];

    let run = SortedRun::new(1, files);

    // Target in first file
    assert_eq!(run.find_file(b"b"), Some(0));

    // Target in second file
    assert_eq!(run.find_file(b"e"), Some(1));

    // Target in third file
    assert_eq!(run.find_file(b"h"), Some(2));

    // Target before all files
    assert_eq!(run.find_file(b"0"), Some(0));

    // Target at boundary
    assert_eq!(run.find_file(b"c"), Some(0));
    assert_eq!(run.find_file(b"d"), Some(1));

    // Target after all files
    assert_eq!(run.find_file(b"z"), None);
}

#[test]
fn test_sorted_run_iterator() {
    let files = vec![
        create_data_file(1, b"a", b"c"),
        create_data_file(2, b"d", b"f"),
    ];

    let run = SortedRun::new(1, files);

    // Create a mock iterator factory
    let create_iter = |file: &DataFile| -> Result<MockIterator> {
        let entries = match file.file_id {
            1 => vec![(b"a".as_slice(), b"v1"), (b"b", b"v2"), (b"c", b"v3")],
            2 => vec![(b"d".as_slice(), b"v4"), (b"e", b"v5"), (b"f", b"v6")],
            _ => vec![],
        };
        Ok(MockIterator::new(entries))
    };

    let mut iter = run.iter(create_iter);
    iter.seek_to_first().unwrap();

    let mut results = vec![];
    while iter.valid() {
        let (k, kv) = iter.take_current().unwrap().unwrap();
        let v = kv.unwrap_encoded();
        results.push((k, v));
        iter.next().unwrap();
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
fn test_sorted_run_iterator_seek() {
    let files = vec![
        create_data_file(1, b"a", b"c"),
        create_data_file(2, b"d", b"f"),
    ];

    let run = SortedRun::new(1, files);

    let create_iter = |file: &DataFile| -> Result<MockIterator> {
        let entries = match file.file_id {
            1 => vec![(b"a".as_slice(), b"v1"), (b"b", b"v2"), (b"c", b"v3")],
            2 => vec![(b"d".as_slice(), b"v4"), (b"e", b"v5"), (b"f", b"v6")],
            _ => vec![],
        };
        Ok(MockIterator::new(entries))
    };

    let mut iter = run.iter(create_iter);

    // Seek to middle of first file
    iter.seek(b"b").unwrap();
    assert!(iter.valid());
    assert_eq!(iter.key().unwrap().unwrap(), b"b");

    // Seek to second file
    iter.seek(b"e").unwrap();
    assert!(iter.valid());
    assert_eq!(iter.key().unwrap().unwrap(), b"e");

    // Seek to exact boundary
    iter.seek(b"d").unwrap();
    assert!(iter.valid());
    assert_eq!(iter.key().unwrap().unwrap(), b"d");
}

#[test]
fn test_sorted_run_seek_only_seeks_loaded_child_once() {
    let files = vec![
        create_data_file(1, b"a", b"c"),
        create_data_file(2, b"d", b"f"),
    ];
    let seek_calls = Arc::new(AtomicUsize::new(0));
    let seek_to_first_calls = Arc::new(AtomicUsize::new(0));
    let seek_calls_for_factory = Arc::clone(&seek_calls);
    let seek_to_first_calls_for_factory = Arc::clone(&seek_to_first_calls);
    let run = SortedRun::new(1, files);
    let mut iter = run.iter(move |file: &DataFile| {
        let entries = if file.file_id == 1 {
            vec![(b"a".as_slice(), b"v1"), (b"b", b"v2")]
        } else {
            vec![(b"d".as_slice(), b"v3"), (b"e", b"v4")]
        };
        Ok(CountingIterator {
            inner: MockIterator::new(entries),
            seek_calls: Arc::clone(&seek_calls_for_factory),
            seek_to_first_calls: Arc::clone(&seek_to_first_calls_for_factory),
        })
    });

    iter.seek(b"b").unwrap();

    assert_eq!(seek_calls.load(Ordering::Relaxed), 1);
    assert_eq!(seek_to_first_calls.load(Ordering::Relaxed), 0);
    assert_eq!(iter.key().unwrap(), Some(b"b".as_slice()));

    assert!(iter.next().unwrap());
    assert_eq!(iter.key().unwrap(), Some(b"d".as_slice()));
    assert_eq!(seek_calls.load(Ordering::Relaxed), 1);
    assert_eq!(seek_to_first_calls.load(Ordering::Relaxed), 1);
}
