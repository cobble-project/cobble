use crate::error::Error;
use bytes::{Bytes, BytesMut};

pub struct FileHandle {
    pub id: u64,
    pub size: u64,
}

pub trait File {
    fn close(&mut self) -> Result<(), Error>;
    fn get_handle(&self) -> &FileHandle;
}

pub trait RandomAccessFile: File {
    fn read_at(&self, offset: usize, size: usize) -> Result<Bytes, Error>;
    
    /// Get the size of the file in bytes
    fn size(&self) -> u64 {
        self.get_handle().size
    }
}

pub trait SequentialWriteFile: File {
    fn write(&mut self, data: &[u8]) -> Result<usize, Error>;
}

// Implement File for Box<dyn SequentialWriteFile>
impl File for Box<dyn SequentialWriteFile> {
    fn close(&mut self) -> Result<(), Error> {
        (**self).close()
    }

    fn get_handle(&self) -> &FileHandle {
        (**self).get_handle()
    }
}

// Implement SequentialWriteFile for Box<dyn SequentialWriteFile>
impl SequentialWriteFile for Box<dyn SequentialWriteFile> {
    fn write(&mut self, data: &[u8]) -> Result<usize, Error> {
        (**self).write(data)
    }
}

/// A buffered reader for efficient random access reads
pub struct BufferedReader<R: RandomAccessFile> {
    inner: R,
    buffer_size: usize,
}

impl<R: RandomAccessFile> BufferedReader<R> {
    pub fn new(inner: R, buffer_size: usize) -> Self {
        Self { inner, buffer_size }
    }

    pub fn read_at(&self, offset: usize, size: usize) -> Result<Bytes, Error> {
        self.inner.read_at(offset, size)
    }
}

/// A buffered writer for efficient sequential writes
pub struct BufferedWriter<W: SequentialWriteFile> {
    inner: W,
    buffer: BytesMut,
    buffer_size: usize,
    offset: usize,
}

impl<W: SequentialWriteFile> BufferedWriter<W> {
    pub fn new(inner: W, buffer_size: usize) -> Self {
        Self {
            inner,
            buffer: BytesMut::with_capacity(buffer_size),
            buffer_size,
            offset: 0,
        }
    }

    pub fn write(&mut self, data: &[u8]) -> Result<usize, Error> {
        // Write data to the buffer. All data is accepted into the buffer,
        // and flushed to the underlying file when the buffer is full.
        // Returns the full length of data written to the buffer.
        let mut written = 0;
        let mut remaining = data;

        while !remaining.is_empty() {
            let available = self.buffer_size - self.buffer.len();
            if available == 0 {
                self.flush()?;
                continue;
            }

            let to_write = remaining.len().min(available);
            self.buffer.extend_from_slice(&remaining[..to_write]);
            written += to_write;
            remaining = &remaining[to_write..];
        }

        Ok(written)
    }

    pub fn flush(&mut self) -> Result<(), Error> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        let data = self.buffer.split();
        let len = data.len();
        self.inner.write(&data)?;
        self.offset += len;
        Ok(())
    }

    pub fn close(mut self) -> Result<(), Error> {
        self.flush()?;
        self.inner.close()
    }

    pub fn offset(&self) -> usize {
        self.offset + self.buffer.len()
    }
}
