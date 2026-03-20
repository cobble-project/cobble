use crate::block_cache::{BlockCache, BlockCacheKey, BlockCacheKind, CachedBlock};
use crate::error::{Error, Result};
use crate::file::{
    BufferedReader, BufferedWriter, File, FileId, RandomAccessFile, SequentialWriteFile,
};
use bytes::Bytes;
use parquet::file::reader::{ChunkReader, Length};
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::{Arc, Mutex};

pub(crate) const PARQUET_READER_BUFFER_SIZE: usize = 8192;

pub(crate) struct SequentialWriteFileAdapter<W: SequentialWriteFile> {
    inner: BufferedWriter<W>,
}

impl<W: SequentialWriteFile> SequentialWriteFileAdapter<W> {
    pub(crate) fn new_buffered(inner: W, buffer_size: usize) -> Self {
        Self {
            inner: BufferedWriter::new(inner, buffer_size),
        }
    }
}

impl<W: SequentialWriteFile> Write for SequentialWriteFileAdapter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.inner.write(buf).map_err(std::io::Error::other)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<W: SequentialWriteFile> File for SequentialWriteFileAdapter<W> {
    fn close(&mut self) -> Result<()> {
        self.inner.close()
    }

    fn size(&self) -> usize {
        self.inner.size()
    }
}

struct SharedRandomAccessFile {
    inner: Arc<dyn RandomAccessFile>,
}

impl File for SharedRandomAccessFile {
    fn close(&mut self) -> Result<()> {
        Ok(())
    }

    fn size(&self) -> usize {
        self.inner.size()
    }
}

impl RandomAccessFile for SharedRandomAccessFile {
    fn read_at(&self, offset: usize, size: usize) -> Result<Bytes> {
        self.inner.read_at(offset, size)
    }
}

pub(crate) struct RandomAccessReadAt {
    inner: Arc<RandomAccessChunkReaderInner>,
    cursor: u64,
}

impl Read for RandomAccessReadAt {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        let remaining = self.inner.file_size.saturating_sub(self.cursor as usize);
        if remaining == 0 {
            return Ok(0);
        }
        let to_read = remaining.min(buf.len());
        let bytes = self
            .inner
            .read_at_cached(self.cursor as usize, to_read)
            .map_err(std::io::Error::other)?;
        buf[..bytes.len()].copy_from_slice(bytes.as_ref());
        self.cursor = self.cursor.saturating_add(bytes.len() as u64);
        Ok(bytes.len())
    }
}

impl Seek for RandomAccessReadAt {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let size = self.inner.file_size as i128;
        let current = self.cursor as i128;
        let next = match pos {
            SeekFrom::Start(offset) => offset as i128,
            SeekFrom::End(offset) => size + offset as i128,
            SeekFrom::Current(offset) => current + offset as i128,
        };
        if next < 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "negative seek offset",
            ));
        }
        self.cursor = next as u64;
        Ok(self.cursor)
    }
}

struct RandomAccessChunkReaderInner {
    buffered_reader: Mutex<BufferedReader<SharedRandomAccessFile>>,
    file_size: usize,
    block_cache: Option<BlockCache>,
    file_id: Option<FileId>,
}

impl RandomAccessChunkReaderInner {
    fn cache_key(&self, offset: usize, length: usize) -> Option<BlockCacheKey> {
        let file_id = self.file_id?;
        let length = u32::try_from(length).ok()?;
        Some(BlockCacheKey {
            file_id,
            block_id: offset as u64,
            kind: BlockCacheKind::ParquetData(length),
        })
    }

    fn read_from_buffered(&self, offset: usize, length: usize) -> Result<Bytes> {
        self.buffered_reader
            .lock()
            .map_err(|_| Error::IoError("Buffered reader lock poisoned".to_string()))?
            .read_at(offset, length)
    }

    fn read_at_cached(&self, offset: usize, length: usize) -> Result<Bytes> {
        if let (Some(cache), Some(cache_key)) =
            (self.block_cache.as_ref(), self.cache_key(offset, length))
        {
            if let Some(cached) = cache.get(&cache_key) {
                return match cached {
                    CachedBlock::ParquetBlock(bytes) => Ok(bytes),
                    CachedBlock::Block(_) | CachedBlock::BloomFilter(_) => Err(Error::IoError(
                        "Parquet cache entry type mismatch".to_string(),
                    )),
                };
            }
            let bytes = self.read_from_buffered(offset, length)?;
            cache.insert(cache_key, CachedBlock::ParquetBlock(bytes.clone()));
            return Ok(bytes);
        }
        self.read_from_buffered(offset, length)
    }
}

pub(crate) struct RandomAccessChunkReader {
    inner: Arc<RandomAccessChunkReaderInner>,
}

impl RandomAccessChunkReader {
    pub(crate) fn from_arc(file: Arc<dyn RandomAccessFile>) -> Self {
        Self::from_arc_with_cache(file, None, None)
    }

    pub(crate) fn from_arc_with_cache(
        file: Arc<dyn RandomAccessFile>,
        file_id: Option<FileId>,
        block_cache: Option<BlockCache>,
    ) -> Self {
        let file_size = file.size();
        let shared = SharedRandomAccessFile { inner: file };
        Self {
            inner: Arc::new(RandomAccessChunkReaderInner {
                buffered_reader: Mutex::new(BufferedReader::new(
                    shared,
                    PARQUET_READER_BUFFER_SIZE,
                )),
                file_size,
                block_cache,
                file_id,
            }),
        }
    }
}

impl Length for RandomAccessChunkReader {
    fn len(&self) -> u64 {
        self.inner.file_size as u64
    }
}

impl ChunkReader for RandomAccessChunkReader {
    type T = RandomAccessReadAt;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        if start > Length::len(self) {
            return Err(parquet::errors::ParquetError::General(format!(
                "invalid read start {} for file len {}",
                start,
                Length::len(self)
            )));
        }
        Ok(RandomAccessReadAt {
            inner: Arc::clone(&self.inner),
            cursor: start,
        })
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<Bytes> {
        let end = start.saturating_add(length as u64);
        if end > Length::len(self) {
            return Err(parquet::errors::ParquetError::General(format!(
                "out of range read start={} len={} file_len={}",
                start,
                length,
                Length::len(self)
            )));
        }
        self.inner
            .read_at_cached(start as usize, length)
            .map_err(|err: Error| parquet::errors::ParquetError::General(err.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block_cache::{BlockCache, BlockCacheKey, CachedBlock};
    use crate::cache::MockCache;
    use crate::file::FileSystemRegistry;
    use crate::parquet::ParquetWriter;
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
        let mut writer = ParquetWriter::new(writer_file).unwrap();
        for i in 0..5000u32 {
            let key = format!("k{:05}", i);
            let value = format!("value_{:05}_abcdefghijklmnopqrstuvwxyz", i);
            writer.add(key.as_bytes(), value.as_bytes()).unwrap();
        }
        writer.finish().unwrap();
        let reader = fs.open_read("test.parquet").unwrap();
        let (counting_reader, read_count) = CountingRandomAccessFile::new(Box::new(reader));
        let reader: Arc<dyn RandomAccessFile> = Arc::new(counting_reader);

        let mock = Arc::new(MockCache::<BlockCacheKey, CachedBlock>::default());
        let cache: BlockCache = mock.clone();
        let chunk_reader =
            RandomAccessChunkReader::from_arc_with_cache(reader, Some(777), Some(cache));
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
        let mut writer = ParquetWriter::new(writer_file).unwrap();
        for i in 0..5000u32 {
            let key = format!("k{:05}", i);
            let value = format!("value_{:05}_abcdefghijklmnopqrstuvwxyz", i);
            writer.add(key.as_bytes(), value.as_bytes()).unwrap();
        }
        writer.finish().unwrap();
        let reader = fs.open_read("test.parquet").unwrap();
        let (counting_reader, read_count) = CountingRandomAccessFile::new(Box::new(reader));
        let reader: Arc<dyn RandomAccessFile> = Arc::new(counting_reader);

        let mock = Arc::new(MockCache::<BlockCacheKey, CachedBlock>::default());
        let cache: BlockCache = mock.clone();
        let chunk_reader =
            RandomAccessChunkReader::from_arc_with_cache(reader, Some(888), Some(cache));
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
}
