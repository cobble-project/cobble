use super::*;
use crate::cache::MockCache;
use crate::data_file::DataFileType;
use crate::file::{File, FileSystemRegistry};
use crate::format::FileBuildResult;
use crate::sst::format::{BlockBuilder, Footer};
use crate::sst::writer::{SSTWriter, SSTWriterOptions};
use std::io::{Read, Seek, SeekFrom, Write};

pub(crate) struct SSTIteratorTestCache {
    inner: SSTIterator,
}

impl SSTIterator {
    pub(crate) fn new(
        file: Box<dyn RandomAccessFile>,
        options: SSTIteratorOptions,
    ) -> Result<Self> {
        Self::with_file_id(file, 0, options)
    }

    pub(crate) fn with_file_id(
        file: Box<dyn RandomAccessFile>,
        file_id: u64,
        options: SSTIteratorOptions,
    ) -> Result<Self> {
        Self::with_cache(file, file_id, options, None, None)
    }

    pub(crate) fn with_cache(
        file: Box<dyn RandomAccessFile>,
        file_id: u64,
        options: SSTIteratorOptions,
        block_cache: Option<BlockCache>,
        footer_bytes: Option<Bytes>,
    ) -> Result<Self> {
        let (iter, _) =
            Self::with_cache_and_footer_bytes(file, file_id, options, block_cache, footer_bytes)?;
        Ok(iter)
    }

    pub(crate) fn with_cache_test(
        file: Box<dyn RandomAccessFile>,
        file_id: u64,
        options: SSTIteratorOptions,
        block_cache: BlockCache,
    ) -> Result<SSTIteratorTestCache> {
        let inner = Self::with_cache(file, file_id, options, Some(block_cache), None)?;
        Ok(SSTIteratorTestCache { inner })
    }
}

impl SSTIteratorTestCache {
    pub(crate) fn seek(&mut self, target: &[u8]) -> Result<()> {
        self.inner.seek(target)
    }

    pub(crate) fn valid(&self) -> bool {
        self.inner.valid()
    }
}

struct BytesRandomAccessFile {
    data: Bytes,
}

impl File for BytesRandomAccessFile {
    fn close(&mut self) -> Result<()> {
        Ok(())
    }

    fn size(&self) -> usize {
        self.data.len()
    }
}

impl RandomAccessFile for BytesRandomAccessFile {
    fn read_at(&self, offset: usize, size: usize) -> Result<Bytes> {
        let end = offset
            .checked_add(size)
            .filter(|&end| end <= self.data.len())
            .ok_or_else(|| Error::IoError("Read beyond test file".to_string()))?;
        Ok(self.data.slice(offset..end))
    }
}

fn test_data_file(file_id: u64, size: usize) -> DataFile {
    DataFile::new_untracked(
        DataFileType::SSTable,
        b"a".to_vec(),
        b"z".to_vec(),
        file_id,
        0,
        size,
        0..=0,
        0..=0,
    )
}

fn test_sst_bytes(partitioned_index: bool, index_value: &[u8]) -> Bytes {
    let mut index_builder = BlockBuilder::new(1024);
    index_builder.add(b"index", index_value);
    let index_block = index_builder.build().encode();
    let footer = Footer {
        index_block_offset: 0,
        index_block_size: index_block.len() as u64,
        filter_block_offset: 0,
        filter_block_size: 0,
        filter_present: false,
        partitioned_index,
        value_has_ttl: true,
        block_checksums: false,
    }
    .encode();
    let mut bytes = BytesMut::with_capacity(index_block.len() + footer.len());
    bytes.extend_from_slice(index_block.as_ref());
    bytes.extend_from_slice(footer.as_ref());
    bytes.freeze()
}

fn test_reader(data: Bytes) -> Box<dyn RandomAccessFile> {
    Box::new(BytesRandomAccessFile { data })
}

#[test]
fn test_data_file_read_metadata_reuses_partitioned_descriptors_and_index_cache() {
    let mut partition = Vec::with_capacity(16);
    partition.extend_from_slice(&128_u64.to_le_bytes());
    partition.extend_from_slice(&64_u64.to_le_bytes());
    let bytes = test_sst_bytes(true, &partition);
    let data_file = test_data_file(100, bytes.len());
    let cache = Arc::new(MockCache::<BlockCacheKey, CachedBlock>::default());
    let block_cache: BlockCache = cache.clone();

    let first = SSTIterator::with_cache_and_file(
        test_reader(bytes.clone()),
        &data_file,
        SSTIteratorOptions {
            read_metadata_cache_mode: SstReadMetadataCacheMode::Lazy,
            ..SSTIteratorOptions::default()
        },
        Some(block_cache.clone()),
    )
    .unwrap();
    let metadata = data_file.sst_read_metadata().unwrap();
    assert_eq!(metadata.index_partitions().as_ref(), &[(128, 64)]);
    assert_eq!(cache.get_count(), 1);
    assert_eq!(cache.insert_count(), 1);

    let second = SSTIterator::with_cache_and_file(
        test_reader(bytes),
        &data_file,
        SSTIteratorOptions {
            read_metadata_cache_mode: SstReadMetadataCacheMode::Lazy,
            ..SSTIteratorOptions::default()
        },
        Some(block_cache),
    )
    .unwrap();
    assert!(Arc::ptr_eq(
        &metadata,
        &data_file.sst_read_metadata().unwrap()
    ));
    assert!(Arc::ptr_eq(
        &first.index_partitions,
        &second.index_partitions
    ));
    assert_eq!(cache.get_count(), 2);
    assert_eq!(cache.insert_count(), 1);
}

#[test]
fn test_data_file_read_metadata_reuses_unpartitioned_descriptor() {
    let bytes = test_sst_bytes(false, b"data-block-location");
    let data_file = test_data_file(101, bytes.len());

    let first = SSTIterator::with_cache_and_file(
        test_reader(bytes.clone()),
        &data_file,
        SSTIteratorOptions::default(),
        None,
    )
    .unwrap();
    let metadata = data_file.sst_read_metadata().unwrap();
    assert_eq!(
        metadata.index_partitions().as_ref(),
        &[(0, (bytes.len() - FOOTER_SIZE) as u64)]
    );

    let second = SSTIterator::with_cache_and_file(
        test_reader(bytes),
        &data_file,
        SSTIteratorOptions::default(),
        None,
    )
    .unwrap();
    assert!(Arc::ptr_eq(
        &first.index_partitions,
        &second.index_partitions
    ));
}

#[test]
fn test_data_file_read_metadata_is_retained_by_derived_files() {
    let mut partition = Vec::with_capacity(16);
    partition.extend_from_slice(&128_u64.to_le_bytes());
    partition.extend_from_slice(&64_u64.to_le_bytes());
    let bytes = test_sst_bytes(true, &partition);
    let data_file = test_data_file(102, bytes.len());
    SSTIterator::with_cache_and_file(
        test_reader(bytes),
        &data_file,
        SSTIteratorOptions::default(),
        None,
    )
    .unwrap();
    let metadata = data_file.sst_read_metadata().unwrap();

    let ranged = data_file.with_effective_bucket_range(0..=0);
    let copied = test_data_file(data_file.file_id, data_file.size);
    copied.copy_meta_from(&data_file);

    assert!(Arc::ptr_eq(&metadata, &ranged.sst_read_metadata().unwrap()));
    assert!(Arc::ptr_eq(&metadata, &copied.sst_read_metadata().unwrap()));
}

#[test]
fn test_data_file_read_metadata_malformed_first_construction_does_not_cache() {
    let mut zero_sized_partition = Vec::with_capacity(16);
    zero_sized_partition.extend_from_slice(&128_u64.to_le_bytes());
    zero_sized_partition.extend_from_slice(&0_u64.to_le_bytes());

    for (file_id, value) in [(104, vec![0; 15]), (105, zero_sized_partition)] {
        let bytes = test_sst_bytes(true, &value);
        let data_file = test_data_file(file_id, bytes.len());
        assert!(
            SSTIterator::with_cache_and_file(
                test_reader(bytes),
                &data_file,
                SSTIteratorOptions::default(),
                None,
            )
            .is_err()
        );
        assert!(data_file.sst_read_metadata().is_none());
        assert!(data_file.meta_bytes().is_none());
    }
}

#[test]
fn test_data_file_read_metadata_cache_can_be_disabled() {
    let mut partition = Vec::with_capacity(16);
    partition.extend_from_slice(&128_u64.to_le_bytes());
    partition.extend_from_slice(&64_u64.to_le_bytes());
    let bytes = test_sst_bytes(true, &partition);
    let data_file = test_data_file(106, bytes.len());
    let options = SSTIteratorOptions {
        read_metadata_cache_mode: SstReadMetadataCacheMode::Off,
        ..SSTIteratorOptions::default()
    };

    SSTIterator::with_cache_and_file(test_reader(bytes), &data_file, options, None).unwrap();

    assert!(data_file.sst_read_metadata().is_none());
}

#[test]
#[serial_test::serial(file)]
fn test_sst_data_block_checksum_detects_corruption() {
    let root = "/tmp/sst_block_checksum_test";
    let _ = std::fs::remove_dir_all(root);
    let registry = FileSystemRegistry::new();
    let fs = registry
        .get_or_register(format!("file://{}", root))
        .unwrap();
    let writer_file = fs.open_write("test.sst").unwrap();
    let mut writer = SSTWriter::new(writer_file, SSTWriterOptions::default());
    writer.add(b"key", b"value").unwrap();
    let FileBuildResult { meta_bytes, .. } = writer.finish_with_range().unwrap();
    assert!(Footer::decode(&meta_bytes).unwrap().block_checksums);

    let path = format!("{}/test.sst", root);
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    file.seek(SeekFrom::Start(8)).unwrap();
    let mut byte = [0u8; 1];
    file.read_exact(&mut byte).unwrap();
    byte[0] ^= 0x80;
    file.seek(SeekFrom::Start(8)).unwrap();
    file.write_all(&byte).unwrap();
    file.flush().unwrap();

    let reader_file = fs.open_read("test.sst").unwrap();
    let mut iter = SSTIterator::new(reader_file, SSTIteratorOptions::default()).unwrap();
    assert!(matches!(
        iter.seek_to_first(),
        Err(Error::ChecksumMismatch(_))
    ));
    let _ = std::fs::remove_dir_all(root);
}

#[test]
#[serial_test::serial(file)]
fn test_sst_data_block_checksum_can_be_disabled() {
    let root = "/tmp/sst_block_checksum_disabled_test";
    let _ = std::fs::remove_dir_all(root);
    let registry = FileSystemRegistry::new();
    let fs = registry
        .get_or_register(format!("file://{}", root))
        .unwrap();
    let writer_file = fs.open_write("test.sst").unwrap();
    let mut writer = SSTWriter::new(
        writer_file,
        SSTWriterOptions {
            block_checksum_enabled: false,
            ..Default::default()
        },
    );
    writer.add(b"key", b"value").unwrap();
    let FileBuildResult { meta_bytes, .. } = writer.finish_with_range().unwrap();
    assert!(!Footer::decode(&meta_bytes).unwrap().block_checksums);

    let reader_file = fs.open_read("test.sst").unwrap();
    let mut iter = SSTIterator::new(reader_file, SSTIteratorOptions::default()).unwrap();
    iter.seek_to_first().unwrap();
    assert_eq!(iter.key().unwrap().unwrap().as_ref(), b"key");
    let _ = std::fs::remove_dir_all(root);
}

#[test]
#[serial_test::serial(file)]
fn test_sst_iterator_basic() {
    let _ = std::fs::remove_dir_all("/tmp/sst_test");
    let registry = FileSystemRegistry::new();
    let fs = registry.get_or_register("file:///tmp/sst_test").unwrap();

    // Write SST file
    {
        let writer_file = fs.open_write("test.sst").unwrap();
        let mut writer = SSTWriter::new(
            writer_file,
            SSTWriterOptions {
                bloom_filter_enabled: true,
                ..SSTWriterOptions::default()
            },
        );

        writer.add(b"key1", b"value1").unwrap();
        writer.add(b"key2", b"value2").unwrap();
        writer.add(b"key3", b"value3").unwrap();

        writer.finish().unwrap();
    }

    // Read SST file
    {
        let reader_file = fs.open_read("test.sst").unwrap();
        let mut iter = SSTIterator::with_cache(
            reader_file,
            0,
            SSTIteratorOptions {
                bloom_filter_enabled: true,
                ..SSTIteratorOptions::default()
            },
            None,
            None,
        )
        .unwrap();

        iter.seek_to_first().unwrap();

        let mut count = 0;
        while iter.valid() {
            let (key, value) = iter.current().unwrap().unwrap();
            count += 1;
            match count {
                1 => {
                    assert_eq!(&key[..], b"key1");
                    assert_eq!(&value[..], b"value1");
                }
                2 => {
                    assert_eq!(&key[..], b"key2");
                    assert_eq!(&value[..], b"value2");
                }
                3 => {
                    assert_eq!(&key[..], b"key3");
                    assert_eq!(&value[..], b"value3");
                }
                _ => panic!("Too many entries"),
            }
            iter.next().unwrap();
        }

        assert_eq!(count, 3);
    }

    let _ = std::fs::remove_dir_all("/tmp/sst_test");
}

#[test]
#[serial_test::serial(file)]
fn test_sst_iterator_with_compression() {
    let _ = std::fs::remove_dir_all("/tmp/sst_compressed_test");
    let registry = FileSystemRegistry::new();
    let fs = registry
        .get_or_register("file:///tmp/sst_compressed_test")
        .unwrap();

    {
        let writer_file = fs.open_write("compressed.sst").unwrap();
        let mut writer = SSTWriter::new(
            writer_file,
            SSTWriterOptions {
                bloom_filter_enabled: true,
                compression: crate::SstCompressionAlgorithm::Lz4,
                ..SSTWriterOptions::default()
            },
        );

        writer.add(b"key1", b"value1").unwrap();
        writer.add(b"key2", b"value2").unwrap();
        writer.finish().unwrap();
    }

    {
        let reader_file = fs.open_read("compressed.sst").unwrap();
        let mut iter = SSTIterator::with_cache(
            reader_file,
            0,
            SSTIteratorOptions {
                bloom_filter_enabled: true,
                ..SSTIteratorOptions::default()
            },
            None,
            None,
        )
        .unwrap();

        iter.seek_to_first().unwrap();
        let (key, value) = iter.current().unwrap().unwrap();
        assert_eq!(&key[..], b"key1");
        assert_eq!(&value[..], b"value1");
    }

    let _ = std::fs::remove_dir_all("/tmp/sst_compressed_test");
}

#[test]
#[serial_test::serial(file)]
fn test_sst_iterator_seek() {
    let _ = std::fs::remove_dir_all("/tmp/sst_test");
    let registry = FileSystemRegistry::new();
    let fs = registry.get_or_register("file:///tmp/sst_test").unwrap();

    // Write SST file
    {
        let writer_file = fs.open_write("test_seek.sst").unwrap();
        let mut writer = SSTWriter::new(
            writer_file,
            SSTWriterOptions {
                bloom_filter_enabled: true,
                ..SSTWriterOptions::default()
            },
        );

        writer.add(b"key0001", b"value0001").unwrap();
        writer.add(b"key0003", b"value0003").unwrap();
        writer.add(b"key0005", b"value0005").unwrap();
        writer.add(b"key0007", b"value0007").unwrap();
        // fill more entries to ensure multiple blocks
        for i in 0..1000 {
            let key = format!("key{:04}", i * 2 + 10);
            let value = format!("value{:04}", i * 2 + 10);
            writer.add(key.as_bytes(), value.as_bytes()).unwrap();
        }

        writer.finish().unwrap();
    }

    // Read and seek
    {
        let reader_file = fs.open_read("test_seek.sst").unwrap();
        let mut iter = SSTIterator::with_cache(
            reader_file,
            0,
            SSTIteratorOptions {
                bloom_filter_enabled: true,
                ..SSTIteratorOptions::default()
            },
            None,
            None,
        )
        .unwrap();

        // Seek to exact key
        iter.seek(b"key0003").unwrap();
        assert!(iter.valid());
        let (key, value) = iter.current().unwrap().unwrap();
        assert_eq!(&key[..], b"key0003");
        assert_eq!(&value[..], b"value0003");

        // Seek to key between entries
        iter.seek(b"key0004").unwrap();
        assert!(iter.valid());
        let (key, value) = iter.current().unwrap().unwrap();
        assert_eq!(&key[..], b"key0005");
        assert_eq!(&value[..], b"value0005");

        // Seek to first
        iter.seek(b"key0000").unwrap();
        assert!(iter.valid());
        let (key, value) = iter.current().unwrap().unwrap();
        assert_eq!(&key[..], b"key0001");
        assert_eq!(&value[..], b"value0001");
    }

    let _ = std::fs::remove_dir_all("/tmp/sst_test");
}

#[test]
#[serial_test::serial(file)]
fn test_sst_iterator_seek_advances_across_data_block_gap() {
    let _ = std::fs::remove_dir_all("/tmp/sst_seek_data_block_gap_test");
    let registry = FileSystemRegistry::new();
    let fs = registry
        .get_or_register("file:///tmp/sst_seek_data_block_gap_test")
        .unwrap();

    {
        let writer_file = fs.open_write("seek_gap.sst").unwrap();
        let mut writer = SSTWriter::new(
            writer_file,
            SSTWriterOptions {
                block_size: 32,
                ..SSTWriterOptions::default()
            },
        );
        writer.add(b"a", &[b'a'; 64]).unwrap();
        writer.add(b"c", &[b'c'; 64]).unwrap();
        writer.finish().unwrap();
    }

    {
        let reader_file = fs.open_read("seek_gap.sst").unwrap();
        let mut iter =
            SSTIterator::with_cache(reader_file, 0, SSTIteratorOptions::default(), None, None)
                .unwrap();

        iter.seek(b"b").unwrap();
        assert!(iter.valid());
        assert_eq!(iter.key().unwrap().unwrap().as_ref(), b"c");
    }

    let _ = std::fs::remove_dir_all("/tmp/sst_seek_data_block_gap_test");
}

#[test]
#[serial_test::serial(file)]
fn test_sst_iterator_prefix_scan_reuses_current_key_across_next() {
    let _ = std::fs::remove_dir_all("/tmp/sst_prefix_scan_test");
    let registry = FileSystemRegistry::new();
    let fs = registry
        .get_or_register("file:///tmp/sst_prefix_scan_test")
        .unwrap();

    {
        let writer_file = fs.open_write("prefix_scan.sst").unwrap();
        let mut writer = SSTWriter::new(
            writer_file,
            SSTWriterOptions {
                data_block_restart_interval: 3,
                ..SSTWriterOptions::default()
            },
        );
        for idx in 0..6 {
            let key = format!("map:key:{idx:04}");
            let value = format!("value:{idx:04}");
            writer.add(key.as_bytes(), value.as_bytes()).unwrap();
        }
        writer.finish().unwrap();
    }

    {
        let reader_file = fs.open_read("prefix_scan.sst").unwrap();
        let mut iter =
            SSTIterator::with_cache(reader_file, 0, SSTIteratorOptions::default(), None, None)
                .unwrap();

        iter.seek_to_first().unwrap();
        for idx in 0..6 {
            let expected_key = format!("map:key:{idx:04}");
            let expected_value = format!("value:{idx:04}");
            let borrowed_key = <SSTIterator as KvIterator>::key(&iter)
                .unwrap()
                .unwrap()
                .to_vec();
            assert_eq!(borrowed_key, expected_key.as_bytes());
            let (current_key, current_value) = iter.current().unwrap().unwrap();
            assert_eq!(current_key.as_ref(), expected_key.as_bytes());
            assert_eq!(current_value.as_ref(), expected_value.as_bytes());
            if idx < 5 {
                assert!(iter.next().unwrap());
            } else {
                assert!(!iter.next().unwrap());
            }
        }
    }

    let _ = std::fs::remove_dir_all("/tmp/sst_prefix_scan_test");
}

#[test]
#[serial_test::serial(file)]
fn test_sst_iterator_can_resume_after_block_boundary_stop() {
    let _ = std::fs::remove_dir_all("/tmp/sst_block_boundary_resume_test");
    let registry = FileSystemRegistry::new();
    let fs = registry
        .get_or_register("file:///tmp/sst_block_boundary_resume_test")
        .unwrap();

    {
        let writer_file = fs.open_write("resume.sst").unwrap();
        let mut writer = SSTWriter::new(
            writer_file,
            SSTWriterOptions {
                block_size: 32,
                data_block_restart_interval: 1,
                ..SSTWriterOptions::default()
            },
        );
        for key in [b"a1", b"b1", b"c1", b"d1"] {
            writer.add(key.as_slice(), b"value").unwrap();
        }
        writer.finish().unwrap();
    }

    {
        let reader_file = fs.open_read("resume.sst").unwrap();
        let mut iter =
            SSTIterator::with_cache(reader_file, 0, SSTIteratorOptions::default(), None, None)
                .unwrap();
        iter.seek_to_first().unwrap();
        iter.set_stop_at_block_boundary(true);

        let mut keys = vec![iter.key().unwrap().unwrap().to_vec()];
        loop {
            if iter.next().unwrap() {
                keys.push(iter.key().unwrap().unwrap().to_vec());
                continue;
            }
            if iter.stopped_at_block_boundary() {
                iter.clear_stop_at_block_boundary();
                continue;
            }
            break;
        }

        assert_eq!(
            keys,
            vec![
                b"a1".to_vec(),
                b"b1".to_vec(),
                b"c1".to_vec(),
                b"d1".to_vec()
            ]
        );
    }

    let _ = std::fs::remove_dir_all("/tmp/sst_block_boundary_resume_test");
}

#[test]
#[serial_test::serial(file)]
fn test_sst_typed_kv() {
    use crate::r#type::{Column, Key, Value, ValueType};

    let _ = std::fs::remove_dir_all("/tmp/sst_typed_kv_test");
    let registry = FileSystemRegistry::new();
    let fs = registry
        .get_or_register("file:///tmp/sst_typed_kv_test")
        .unwrap();

    let num_columns = 2;

    // Write SST file using typed Key/Value API
    {
        let writer_file = fs.open_write("typed.sst").unwrap();
        let mut writer = SSTWriter::new(
            writer_file,
            SSTWriterOptions {
                num_columns,
                bloom_filter_enabled: true,
                ..SSTWriterOptions::default()
            },
        );

        let key1 = Key::new(1, b"user:1".to_vec());
        let value1 = Value::new(vec![
            Some(Column::new(ValueType::Put, b"Alice".to_vec())),
            Some(Column::new(ValueType::Put, b"alice@example.com".to_vec())),
        ]);
        writer.add_kv(&key1, &value1).unwrap();

        let key2 = Key::new(1, b"user:2".to_vec());
        // user:2 has no email (optional column)
        let value2 = Value::new(vec![
            Some(Column::new(ValueType::Put, b"Bob".to_vec())),
            None,
        ]);
        writer.add_kv(&key2, &value2).unwrap();

        let key3 = Key::new(2, b"order:100".to_vec());
        let value3 = Value::new(vec![
            Some(Column::new(ValueType::Delete, b"".to_vec())),
            None,
        ]);
        writer.add_kv(&key3, &value3).unwrap();

        writer.finish().unwrap();
    }

    // Read SST file using typed Key/Value API
    {
        let reader_file = fs.open_read("typed.sst").unwrap();
        let mut iter = SSTIterator::with_cache(
            reader_file,
            0,
            SSTIteratorOptions {
                bloom_filter_enabled: true,
                num_columns,
                ..SSTIteratorOptions::default()
            },
            None,
            None,
        )
        .unwrap();

        iter.seek_to_first().unwrap();

        // First entry
        assert!(iter.valid());
        let (key, value) = iter.current_kv().unwrap().unwrap();
        let cols = value.columns();
        assert_eq!(key.bucket(), 1);
        assert_eq!(key.data().as_ref(), b"user:1");
        assert!(cols[0].is_some());
        assert_eq!(cols[0].as_ref().unwrap().data().as_ref(), b"Alice");
        assert!(cols[1].is_some());
        assert_eq!(
            cols[1].as_ref().unwrap().data().as_ref(),
            b"alice@example.com"
        );

        // Second entry
        iter.next().unwrap();
        assert!(iter.valid());
        let key = iter.current_key().unwrap().unwrap();
        let value = iter.current_value().unwrap().unwrap();
        let cols = value.columns();
        assert_eq!(key.bucket(), 1);
        assert_eq!(key.data().as_ref(), b"user:2");
        assert!(cols[0].is_some());
        assert_eq!(cols[0].as_ref().unwrap().data().as_ref(), b"Bob");
        assert!(cols[1].is_none());

        // Third entry
        iter.next().unwrap();
        assert!(iter.valid());
        let (key, value) = iter.current_kv().unwrap().unwrap();
        let cols = value.columns();
        assert_eq!(key.bucket(), 2);
        assert_eq!(key.data().as_ref(), b"order:100");
        assert!(cols[0].is_some());
        assert!(matches!(
            cols[0].as_ref().unwrap().value_type(),
            ValueType::Delete
        ));

        // No more entries
        iter.next().unwrap();
        assert!(!iter.valid());
    }

    // Test seek_key
    {
        let reader_file = fs.open_read("typed.sst").unwrap();
        let mut iter = SSTIterator::with_cache(
            reader_file,
            0,
            SSTIteratorOptions {
                bloom_filter_enabled: true,
                num_columns,
                ..SSTIteratorOptions::default()
            },
            None,
            None,
        )
        .unwrap();

        let target = Key::new(1, b"user:2".to_vec());
        iter.seek_key(&target).unwrap();
        assert!(iter.valid());
        let key = iter.current_key().unwrap().unwrap();
        assert_eq!(key.data().as_ref(), b"user:2");
    }

    let _ = std::fs::remove_dir_all("/tmp/sst_typed_kv_test");
}

#[test]
#[serial_test::serial(file)]
fn test_sst_typed_kv_without_ttl_header() {
    use crate::r#type::{Column, Key, Value, ValueType};

    let _ = std::fs::remove_dir_all("/tmp/sst_typed_kv_no_ttl_test");
    let registry = FileSystemRegistry::new();
    let fs = registry
        .get_or_register("file:///tmp/sst_typed_kv_no_ttl_test")
        .unwrap();

    {
        let writer_file = fs.open_write("typed_no_ttl.sst").unwrap();
        let mut writer = SSTWriter::new(
            writer_file,
            SSTWriterOptions {
                num_columns: 1,
                value_has_ttl: false,
                ..SSTWriterOptions::default()
            },
        );
        let key = Key::new(1, b"user:1".to_vec());
        let value = Value::new_with_expired_at(
            vec![Some(Column::new(ValueType::Put, b"Alice".to_vec()))],
            Some(12345),
        );
        writer.add_kv(&key, &value).unwrap();
        writer.finish().unwrap();
    }

    {
        let reader_file = fs.open_read("typed_no_ttl.sst").unwrap();
        let mut iter = SSTIterator::with_cache(
            reader_file,
            0,
            SSTIteratorOptions {
                num_columns: 1,
                ..SSTIteratorOptions::default()
            },
            None,
            None,
        )
        .unwrap();
        iter.seek_to_first().unwrap();
        let value = iter.current_value().unwrap().unwrap();
        assert_eq!(value.expired_at(), None);
        assert_eq!(
            value.columns()[0].as_ref().unwrap().data().as_ref(),
            b"Alice"
        );
    }

    let _ = std::fs::remove_dir_all("/tmp/sst_typed_kv_no_ttl_test");
}
