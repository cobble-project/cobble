use crate::error::{Error, Result};
use crate::file::{BufferedReader, BufferedWriter, File, RandomAccessFile, SequentialWriteFile};
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
    buffered_reader: Arc<Mutex<BufferedReader<SharedRandomAccessFile>>>,
    file_size: usize,
    cursor: u64,
}

impl Read for RandomAccessReadAt {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        let remaining = self.file_size.saturating_sub(self.cursor as usize);
        if remaining == 0 {
            return Ok(0);
        }
        let to_read = remaining.min(buf.len());
        let bytes = self
            .buffered_reader
            .lock()
            .map_err(|_| std::io::Error::other("Buffered reader lock poisoned"))?
            .read_at(self.cursor as usize, to_read)
            .map_err(std::io::Error::other)?;
        buf[..bytes.len()].copy_from_slice(bytes.as_ref());
        self.cursor = self.cursor.saturating_add(bytes.len() as u64);
        Ok(bytes.len())
    }
}

impl Seek for RandomAccessReadAt {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let size = self.file_size as i128;
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

pub(crate) struct RandomAccessChunkReader {
    buffered_reader: Arc<Mutex<BufferedReader<SharedRandomAccessFile>>>,
    file_size: usize,
}

impl RandomAccessChunkReader {
    pub(crate) fn from_arc(file: Arc<dyn RandomAccessFile>) -> Self {
        let file_size = file.size();
        let shared = SharedRandomAccessFile { inner: file };
        Self {
            buffered_reader: Arc::new(Mutex::new(BufferedReader::new(
                shared,
                PARQUET_READER_BUFFER_SIZE,
            ))),
            file_size,
        }
    }
}

impl Length for RandomAccessChunkReader {
    fn len(&self) -> u64 {
        self.file_size as u64
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
            buffered_reader: Arc::clone(&self.buffered_reader),
            file_size: self.file_size,
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
        self.buffered_reader
            .lock()
            .map_err(|_| {
                parquet::errors::ParquetError::General("Buffered reader lock poisoned".to_string())
            })?
            .read_at(start as usize, length)
            .map_err(|err: Error| parquet::errors::ParquetError::General(err.to_string()))
    }
}
