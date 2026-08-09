use super::*;
use crate::cache::MockCache;
use crate::cache::{BlockCache, BlockCacheKey, CachedBlock};
use crate::file::FileSystemRegistry;
use crate::parquet::ParquetWriter;
use crate::sst::row_codec::encode_value;
use crate::r#type::{Column, Value, ValueType};
use parquet::file::reader::ChunkReader;
use std::sync::atomic::{AtomicUsize, Ordering};

struct CountingRandomAccessFile {
    inner: Box<dyn RandomAccessFile>,
    file_size: usize,
    read_count: Arc<AtomicUsize>,
}

impl CountingRandomAccessFile {
    fn new(inner: Box<dyn RandomAccessFile>) -> (Self, Arc<AtomicUsize>) {
        let read_count = Arc::new(AtomicUsize::new(0));
        let file_size = inner.size();
        (
            Self {
                inner,
                file_size,
                read_count: Arc::clone(&read_count),
            },
            read_count,
        )
    }
}

impl File for CountingRandomAccessFile {
    fn close(&mut self) -> Result<()> {
        self.inner.close()
    }

    fn size(&self) -> usize {
        self.file_size
    }
}

impl RandomAccessFile for CountingRandomAccessFile {
    fn prefers_read_ahead(&self) -> bool {
        self.inner.prefers_read_ahead()
    }

    fn read_at(&self, offset: usize, size: usize) -> Result<Bytes> {
        self.read_count.fetch_add(1, Ordering::Relaxed);
        self.inner.read_at(offset, size)
    }
}

#[test]
#[serial_test::serial(file)]
fn test_chunk_reader_get_bytes_uses_cache_across_buffer_switches() {
    let _ = std::fs::remove_dir_all("/tmp/parquet_cache_bytes_test");
    let registry = FileSystemRegistry::new();
    let fs = registry
        .get_or_register("file:///tmp/parquet_cache_bytes_test")
        .unwrap();
    let writer_file = fs.open_write("test.parquet").unwrap();
    let mut writer = ParquetWriter::with_options(
        writer_file,
        crate::parquet::ParquetWriterOptions {
            num_columns: 1,
            ..crate::parquet::ParquetWriterOptions::default()
        },
    )
    .unwrap();
    for i in 0..5000u32 {
        let key = format!("k{:05}", i);
        let value = format!("value_{:05}_abcdefghijklmnopqrstuvwxyz", i);
        let encoded = encode_value(
            &Value::new(vec![Some(Column::new(
                ValueType::Put,
                value.as_bytes().to_vec(),
            ))]),
            1,
        );
        writer.add(key.as_bytes(), &encoded).unwrap();
    }
    writer.finish().unwrap();
    let reader = fs.open_read("test.parquet").unwrap();
    let (counting_reader, read_count) = CountingRandomAccessFile::new(Box::new(reader));
    let reader: Arc<dyn RandomAccessFile> = Arc::new(counting_reader);

    let mock = Arc::new(MockCache::<BlockCacheKey, CachedBlock>::default());
    let cache: BlockCache = mock.clone();
    let chunk_reader = RandomAccessChunkReader::from_arc_with_cache(reader, Some(777), Some(cache));
    assert!(chunk_reader.len() > 20_000);
    let offset_a = 0u64;
    let offset_b = 16_384u64;
    let length = 64usize;

    let a1 = chunk_reader.get_bytes(offset_a, length).unwrap();
    let _ = chunk_reader.get_bytes(offset_b, length).unwrap();
    let a2 = chunk_reader.get_bytes(offset_a, length).unwrap();
    assert_eq!(a1, a2);
    assert_eq!(read_count.load(Ordering::Relaxed), 2);
    assert!(mock.get_count() > 0);
    assert!(mock.insert_count() > 0);
    let _ = std::fs::remove_dir_all("/tmp/parquet_cache_bytes_test");
}

#[test]
#[serial_test::serial(file)]
fn test_chunk_reader_get_read_uses_cache_across_buffer_switches() {
    let _ = std::fs::remove_dir_all("/tmp/parquet_cache_read_test");
    let registry = FileSystemRegistry::new();
    let fs = registry
        .get_or_register("file:///tmp/parquet_cache_read_test")
        .unwrap();
    let writer_file = fs.open_write("test.parquet").unwrap();
    let mut writer = ParquetWriter::with_options(
        writer_file,
        crate::parquet::ParquetWriterOptions {
            num_columns: 1,
            ..crate::parquet::ParquetWriterOptions::default()
        },
    )
    .unwrap();
    for i in 0..5000u32 {
        let key = format!("k{:05}", i);
        let value = format!("value_{:05}_abcdefghijklmnopqrstuvwxyz", i);
        let encoded = encode_value(
            &Value::new(vec![Some(Column::new(
                ValueType::Put,
                value.as_bytes().to_vec(),
            ))]),
            1,
        );
        writer.add(key.as_bytes(), &encoded).unwrap();
    }
    writer.finish().unwrap();
    let reader = fs.open_read("test.parquet").unwrap();
    let (counting_reader, read_count) = CountingRandomAccessFile::new(Box::new(reader));
    let reader: Arc<dyn RandomAccessFile> = Arc::new(counting_reader);

    let mock = Arc::new(MockCache::<BlockCacheKey, CachedBlock>::default());
    let cache: BlockCache = mock.clone();
    let chunk_reader = RandomAccessChunkReader::from_arc_with_cache(reader, Some(888), Some(cache));
    assert!(chunk_reader.len() > 20_000);
    let offset_a = 0u64;
    let offset_b = 16_384u64;
    let mut buf1 = [0u8; 64];
    let mut buf2 = [0u8; 64];

    let mut read_a1 = chunk_reader.get_read(offset_a).unwrap();
    read_a1.read_exact(&mut buf1).unwrap();
    let mut read_b = chunk_reader.get_read(offset_b).unwrap();
    let mut throwaway = [0u8; 64];
    read_b.read_exact(&mut throwaway).unwrap();
    let mut read_a2 = chunk_reader.get_read(offset_a).unwrap();
    read_a2.read_exact(&mut buf2).unwrap();

    assert_eq!(buf1, buf2);
    assert_eq!(read_count.load(Ordering::Relaxed), 2);
    assert!(mock.get_count() > 0);
    assert!(mock.insert_count() > 0);
    let _ = std::fs::remove_dir_all("/tmp/parquet_cache_read_test");
}
