use super::*;
use crate::cache::CacheHandle;
use crate::data_file::DataFileType;
use crate::file::{File, FileSystemRegistry};
use crate::sst::{SSTIterator, SSTWriter, SSTWriterOptions};
use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Default)]
struct RecordingCache {
    entries: Mutex<HashMap<BlockCacheKey, CachedBlock>>,
    gets: Mutex<Vec<BlockCacheKind>>,
}

impl RecordingCache {
    fn clear_history(&self) {
        self.gets.lock().unwrap().clear();
    }

    fn requested_kinds(&self) -> Vec<BlockCacheKind> {
        self.gets.lock().unwrap().clone()
    }
}

impl CacheHandle<BlockCacheKey, CachedBlock> for RecordingCache {
    fn get(&self, key: &BlockCacheKey) -> Option<CachedBlock> {
        self.gets.lock().unwrap().push(key.kind);
        self.entries.lock().unwrap().get(key).cloned()
    }

    fn insert(&self, key: BlockCacheKey, value: CachedBlock) {
        self.entries.lock().unwrap().insert(key, value);
    }

    fn remove(&self, key: &BlockCacheKey) {
        self.entries.lock().unwrap().remove(key);
    }

    fn clear(&self) {
        self.entries.lock().unwrap().clear();
    }
}

struct RecordingRandomAccessFile {
    inner: Box<dyn RandomAccessFile>,
    reads: Arc<Mutex<Vec<(usize, usize)>>>,
}

impl File for RecordingRandomAccessFile {
    fn close(&mut self) -> Result<()> {
        self.inner.close()
    }

    fn size(&self) -> usize {
        self.inner.size()
    }
}

impl RandomAccessFile for RecordingRandomAccessFile {
    fn read_at(&self, offset: usize, size: usize) -> Result<Bytes> {
        self.reads.lock().unwrap().push((offset, size));
        self.inner.read_at(offset, size)
    }
}

fn write_sst(
    partitioned_index: bool,
) -> (
    tempfile::TempDir,
    Arc<dyn crate::file::FileSystem>,
    DataFile,
) {
    write_sst_with_options(
        partitioned_index,
        crate::SstCompressionAlgorithm::None,
        false,
    )
}

fn write_sst_with_options(
    partitioned_index: bool,
    compression: crate::SstCompressionAlgorithm,
    block_checksum_enabled: bool,
) -> (
    tempfile::TempDir,
    Arc<dyn crate::file::FileSystem>,
    DataFile,
) {
    let directory = tempfile::tempdir().unwrap();
    let registry = FileSystemRegistry::new();
    let fs = registry
        .get_or_register(format!("file://{}", directory.path().display()))
        .unwrap();
    let mut writer = SSTWriter::new(
        fs.open_write("point-read.sst").unwrap(),
        SSTWriterOptions {
            block_size: 32,
            bloom_filter_enabled: true,
            bloom_bits_per_key: 100,
            partitioned_index,
            compression,
            block_checksum_enabled,
            ..SSTWriterOptions::default()
        },
    );
    for key in [b"key000".as_slice(), b"key001", b"key002", b"key003"] {
        writer.add(key, b"value").unwrap();
    }
    writer.finish().unwrap();
    let size = fs.open_read("point-read.sst").unwrap().size();
    let data_file = DataFile::new_untracked(
        DataFileType::SSTable,
        b"key000".to_vec(),
        b"key003".to_vec(),
        73,
        0,
        size,
        0..=0,
        0..=0,
    );
    (directory, fs, data_file)
}

fn write_partitioned_sst() -> (
    tempfile::TempDir,
    Arc<dyn crate::file::FileSystem>,
    DataFile,
) {
    write_sst(true)
}

fn options(pin_metadata: bool, pin_metadata_partitions: bool) -> SSTIteratorOptions {
    SSTIteratorOptions {
        bloom_filter_enabled: true,
        pin_metadata,
        pin_metadata_partitions,
        ..SSTIteratorOptions::default()
    }
}

fn get_exact_many_with_default_namespace(
    file: Box<dyn RandomAccessFile>,
    data_file: &DataFile,
    options: SSTIteratorOptions,
    block_cache: Option<BlockCache>,
    keys: &[&[u8]],
) -> Result<Vec<Option<Bytes>>> {
    let data_cache_namespaces = vec![options.cache_namespace; keys.len()];
    SSTPointReader::get_exact_many(
        file,
        data_file,
        options,
        block_cache,
        keys,
        &data_cache_namespaces,
    )
}

fn assert_only_data_block_cache_accesses(cache: &RecordingCache) {
    let kinds = cache.requested_kinds();
    assert!(!kinds.is_empty());
    assert!(kinds.iter().all(|kind| *kind == BlockCacheKind::Data));
}

#[test]
fn point_reader_many_deduplicates_data_blocks_and_coalesces_adjacent_reads() {
    let (_directory, fs, data_file) = write_sst(false);
    let reader = fs.open_read("point-read.sst").unwrap();
    let pinned = PinnedSstReadMetadata::get_or_load(reader.as_ref(), &data_file, true, true)
        .unwrap()
        .unwrap();
    assert!(
        pinned.index_top().get(1).is_ok(),
        "test SST needs multiple data blocks"
    );

    let reads = Arc::new(Mutex::new(Vec::new()));
    let keys = [
        b"key000".as_slice(),
        b"key001".as_slice(),
        b"key001".as_slice(),
        b"key002".as_slice(),
        b"key003".as_slice(),
    ];
    let values = get_exact_many_with_default_namespace(
        Box::new(RecordingRandomAccessFile {
            inner: fs.open_read("point-read.sst").unwrap(),
            reads: Arc::clone(&reads),
        }),
        &data_file,
        options(true, true),
        None,
        &keys,
    )
    .unwrap();

    assert_eq!(
        values,
        vec![
            Some(Bytes::from_static(b"value")),
            Some(Bytes::from_static(b"value")),
            Some(Bytes::from_static(b"value")),
            Some(Bytes::from_static(b"value")),
            Some(Bytes::from_static(b"value")),
        ]
    );
    let reads = reads.lock().unwrap();
    assert_eq!(reads.len(), 1, "all uncached adjacent data blocks coalesce");
    assert_eq!(
        reads[0].0, 0,
        "the first requested data block starts the range"
    );
    assert!(reads[0].1 > 0);
}

#[test]
fn point_reader_many_reuses_cached_data_blocks() {
    let (_directory, fs, data_file) = write_sst(false);
    let reader = fs.open_read("point-read.sst").unwrap();
    PinnedSstReadMetadata::get_or_load(reader.as_ref(), &data_file, true, true)
        .unwrap()
        .unwrap();
    let recording_cache = Arc::new(RecordingCache::default());
    let block_cache: BlockCache = recording_cache.clone();
    let reads = Arc::new(Mutex::new(Vec::new()));
    let keys = [
        b"key000".as_slice(),
        b"key001".as_slice(),
        b"key001".as_slice(),
    ];

    get_exact_many_with_default_namespace(
        Box::new(RecordingRandomAccessFile {
            inner: fs.open_read("point-read.sst").unwrap(),
            reads: Arc::clone(&reads),
        }),
        &data_file,
        options(true, true),
        Some(block_cache.clone()),
        &keys,
    )
    .unwrap();
    assert_eq!(reads.lock().unwrap().len(), 1);
    assert_only_data_block_cache_accesses(&recording_cache);

    reads.lock().unwrap().clear();
    recording_cache.clear_history();
    get_exact_many_with_default_namespace(
        Box::new(RecordingRandomAccessFile {
            inner: fs.open_read("point-read.sst").unwrap(),
            reads: Arc::clone(&reads),
        }),
        &data_file,
        options(true, true),
        Some(block_cache),
        &keys,
    )
    .unwrap();
    assert!(
        reads.lock().unwrap().is_empty(),
        "cached data needs no file I/O"
    );
    assert_only_data_block_cache_accesses(&recording_cache);
}

#[test]
fn point_reader_many_aliases_one_data_block_into_each_cache_namespace() {
    let (_directory, fs, data_file) = write_sst(false);
    let reader = fs.open_read("point-read.sst").unwrap();
    PinnedSstReadMetadata::get_or_load(reader.as_ref(), &data_file, true, true)
        .unwrap()
        .unwrap();
    let recording_cache = Arc::new(RecordingCache::default());
    let block_cache: BlockCache = recording_cache.clone();
    let reads = Arc::new(Mutex::new(Vec::new()));
    let namespace_a = 101;
    let namespace_b = 202;
    let batch_options = SSTIteratorOptions {
        cache_namespace: 999,
        ..options(true, true)
    };

    SSTPointReader::get_exact_many(
        Box::new(RecordingRandomAccessFile {
            inner: fs.open_read("point-read.sst").unwrap(),
            reads: Arc::clone(&reads),
        }),
        &data_file,
        batch_options,
        Some(block_cache.clone()),
        &[b"key000".as_slice(), b"key000".as_slice()],
        &[namespace_a, namespace_b],
    )
    .unwrap();
    assert_eq!(reads.lock().unwrap().len(), 1);

    for namespace in [namespace_a, namespace_b] {
        reads.lock().unwrap().clear();
        get_exact_many_with_default_namespace(
            Box::new(RecordingRandomAccessFile {
                inner: fs.open_read("point-read.sst").unwrap(),
                reads: Arc::clone(&reads),
            }),
            &data_file,
            SSTIteratorOptions {
                cache_namespace: namespace,
                ..options(true, true)
            },
            Some(block_cache.clone()),
            &[b"key000".as_slice()],
        )
        .unwrap();
        assert!(
            reads.lock().unwrap().is_empty(),
            "namespace {namespace} should reuse the aliased data block"
        );
    }
}

#[test]
fn point_reader_many_supports_partitioned_index_and_filter() {
    let (_directory, fs, data_file) = write_partitioned_sst();
    let keys = [
        b"key000".as_slice(),
        b"missing".as_slice(),
        b"key003".as_slice(),
        b"key000".as_slice(),
    ];

    assert_eq!(
        get_exact_many_with_default_namespace(
            fs.open_read("point-read.sst").unwrap(),
            &data_file,
            options(false, false),
            None,
            &keys,
        )
        .unwrap(),
        vec![
            Some(Bytes::from_static(b"value")),
            None,
            Some(Bytes::from_static(b"value")),
            Some(Bytes::from_static(b"value")),
        ]
    );
}

#[test]
fn point_reader_many_decodes_lz4_blocks_with_checksums() {
    let (_directory, fs, data_file) =
        write_sst_with_options(true, crate::SstCompressionAlgorithm::Lz4, true);
    let keys = [
        b"key000".as_slice(),
        b"key003".as_slice(),
        b"key001".as_slice(),
    ];

    assert_eq!(
        get_exact_many_with_default_namespace(
            fs.open_read("point-read.sst").unwrap(),
            &data_file,
            options(false, false),
            None,
            &keys,
        )
        .unwrap(),
        vec![
            Some(Bytes::from_static(b"value")),
            Some(Bytes::from_static(b"value")),
            Some(Bytes::from_static(b"value")),
        ]
    );
}

#[test]
fn point_reader_hits_and_misses_without_constructing_scan_state() {
    let (_directory, fs, data_file) = write_partitioned_sst();

    assert_eq!(
        SSTPointReader::get_exact(
            fs.open_read("point-read.sst").unwrap(),
            &data_file,
            options(false, false),
            None,
            b"key002",
        )
        .unwrap()
        .as_deref(),
        Some(b"value".as_slice())
    );
    assert!(
        SSTPointReader::get_exact(
            fs.open_read("point-read.sst").unwrap(),
            &data_file,
            options(false, false),
            None,
            b"missing",
        )
        .unwrap()
        .is_none()
    );
}

#[test]
fn point_reader_supports_unpartitioned_index_and_filter() {
    let (_directory, fs, data_file) = write_sst(false);

    assert_eq!(
        SSTPointReader::get_exact(
            fs.open_read("point-read.sst").unwrap(),
            &data_file,
            options(true, false),
            None,
            b"key001",
        )
        .unwrap()
        .as_deref(),
        Some(b"value".as_slice())
    );
    assert!(data_file.pinned_sst_read_metadata().is_some());
}

#[test]
fn point_read_pin_is_reused_by_scan_and_compaction_style_iterators() {
    let (_directory, fs, data_file) = write_partitioned_sst();
    let recording_cache = Arc::new(RecordingCache::default());
    let block_cache: BlockCache = recording_cache.clone();

    assert_eq!(
        SSTPointReader::get_exact(
            fs.open_read("point-read.sst").unwrap(),
            &data_file,
            options(true, true),
            Some(block_cache.clone()),
            b"key002",
        )
        .unwrap()
        .as_deref(),
        Some(b"value".as_slice())
    );
    assert!(data_file.pinned_sst_read_metadata().is_some());
    assert_only_data_block_cache_accesses(&recording_cache);

    recording_cache.clear_history();
    let mut scan_iter = SSTIterator::with_cache_and_file(
        fs.open_read("point-read.sst").unwrap(),
        &data_file,
        options(false, true),
        Some(block_cache.clone()),
    )
    .unwrap();
    scan_iter.seek_to_first().unwrap();
    assert!(scan_iter.valid());
    assert_only_data_block_cache_accesses(&recording_cache);

    recording_cache.clear_history();
    let mut compaction_iter = SSTIterator::with_cache_and_file(
        fs.open_read("point-read.sst").unwrap(),
        &data_file,
        SSTIteratorOptions {
            observe_hot_blocks: true,
            ..options(false, true)
        },
        Some(block_cache),
    )
    .unwrap();
    compaction_iter.seek_to_first().unwrap();
    assert!(compaction_iter.valid());
    assert_only_data_block_cache_accesses(&recording_cache);
}

#[test]
fn scan_created_pin_is_reused_by_point_read() {
    let (_directory, fs, data_file) = write_partitioned_sst();
    let recording_cache = Arc::new(RecordingCache::default());
    let block_cache: BlockCache = recording_cache.clone();

    let mut scan_iter = SSTIterator::with_cache_and_file(
        fs.open_read("point-read.sst").unwrap(),
        &data_file,
        options(true, true),
        Some(block_cache.clone()),
    )
    .unwrap();
    scan_iter.seek_to_first().unwrap();
    let mut keys = Vec::new();
    while scan_iter.valid() {
        keys.push(scan_iter.key().unwrap().unwrap().to_vec());
        scan_iter.next().unwrap();
    }
    assert_eq!(
        keys,
        [b"key000", b"key001", b"key002", b"key003"]
            .into_iter()
            .map(|key| key.to_vec())
            .collect::<Vec<_>>()
    );
    assert!(data_file.pinned_sst_read_metadata().is_some());
    assert_only_data_block_cache_accesses(&recording_cache);

    recording_cache.clear_history();
    assert_eq!(
        SSTPointReader::get_exact(
            fs.open_read("point-read.sst").unwrap(),
            &data_file,
            options(false, true),
            Some(block_cache),
            b"key002",
        )
        .unwrap()
        .as_deref(),
        Some(b"value".as_slice())
    );
    assert_only_data_block_cache_accesses(&recording_cache);
}

#[test]
fn metadata_pin_level_gate_can_leave_a_file_unpinned() {
    let (_directory, fs, data_file) = write_partitioned_sst();
    let reader = fs.open_read("point-read.sst").unwrap();

    assert!(
        PinnedSstReadMetadata::get_or_load(reader.as_ref(), &data_file, false, false)
            .unwrap()
            .is_none()
    );
    assert!(data_file.pinned_sst_read_metadata().is_none());
    let reader = fs.open_read("point-read.sst").unwrap();
    assert!(
        PinnedSstReadMetadata::get_or_load(reader.as_ref(), &data_file, true, false)
            .unwrap()
            .is_some()
    );
}

#[test]
fn existing_pin_wins_over_a_later_ineligible_path() {
    let (_directory, fs, data_file) = write_partitioned_sst();
    let reader = fs.open_read("point-read.sst").unwrap();
    let pin = PinnedSstReadMetadata::get_or_load(reader.as_ref(), &data_file, true, false)
        .unwrap()
        .unwrap();
    let reader = fs.open_read("point-read.sst").unwrap();
    let reused = PinnedSstReadMetadata::get_or_load(reader.as_ref(), &data_file, false, true)
        .unwrap()
        .unwrap();

    assert!(Arc::ptr_eq(&pin, &reused));
}

#[test]
fn top_level_pin_keeps_partition_metadata_in_the_block_cache() {
    let (_directory, fs, data_file) = write_partitioned_sst();
    let recording_cache = Arc::new(RecordingCache::default());
    let block_cache: BlockCache = recording_cache.clone();

    assert_eq!(
        SSTPointReader::get_exact(
            fs.open_read("point-read.sst").unwrap(),
            &data_file,
            options(true, false),
            Some(block_cache.clone()),
            b"key002",
        )
        .unwrap()
        .as_deref(),
        Some(b"value".as_slice())
    );

    let kinds = recording_cache.requested_kinds();
    assert!(kinds.contains(&BlockCacheKind::IndexPartition));
    assert!(kinds.contains(&BlockCacheKind::FilterPartition));
    assert!(kinds.contains(&BlockCacheKind::Data));
    assert!(!kinds.contains(&BlockCacheKind::FilterIndex));

    recording_cache.clear_history();
    let mut scan_iter = SSTIterator::with_cache_and_file(
        fs.open_read("point-read.sst").unwrap(),
        &data_file,
        options(false, false),
        Some(block_cache),
    )
    .unwrap();
    scan_iter.seek_to_first().unwrap();
    assert!(scan_iter.valid());
    let kinds = recording_cache.requested_kinds();
    assert!(kinds.contains(&BlockCacheKind::IndexPartition));
    assert!(!kinds.contains(&BlockCacheKind::FilterIndex));
}
