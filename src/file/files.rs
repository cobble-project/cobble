use crate::error::Error;
use bytes::{Bytes, BytesMut};

pub struct FileHandle {
    pub id: u64,
    pub size: usize,
}

pub trait File {
    fn close(&mut self) -> Result<(), Error>;
    fn get_handle(&self) -> &FileHandle;
    
    /// Get the size of the file in bytes
    fn size(&self) -> usize {
        self.get_handle().size
    }
}

pub trait RandomAccessFile: File {
    fn read_at(&self, offset: usize, size: usize) -> Result<Bytes, Error>;
}

pub trait SequentialWriteFile: File {
    fn write(&mut self, data: &[u8]) -> Result<usize, Error>;
}

// Implement File for Box<dyn RandomAccessFile>
impl File for Box<dyn RandomAccessFile> {
    fn close(&mut self) -> Result<(), Error> {
        (**self).close()
    }

    fn get_handle(&self) -> &FileHandle {
        (**self).get_handle()
    }
}

// Implement RandomAccessFile for Box<dyn RandomAccessFile>
impl RandomAccessFile for Box<dyn RandomAccessFile> {
    fn read_at(&self, offset: usize, size: usize) -> Result<Bytes, Error> {
        (**self).read_at(offset, size)
    }
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
    buffer: Bytes,
    buffer_offset: usize,
    buffer_size: usize,
}

impl<R: RandomAccessFile> BufferedReader<R> {
    pub fn new(inner: R, buffer_size: usize) -> Self {
        Self { 
            inner, 
            buffer: Bytes::new(),
            buffer_offset: 0,
            buffer_size,
        }
    }

    pub fn read_at(&mut self, offset: usize, size: usize) -> Result<Bytes, Error> {
        // Check if the requested data is in the buffer
        let buffer_end = self.buffer_offset + self.buffer.len();
        
        if offset >= self.buffer_offset && offset + size <= buffer_end {
            // Data is fully in buffer
            let start = offset - self.buffer_offset;
            return Ok(self.buffer.slice(start..start + size));
        }
        
        // Data is not in buffer or partially in buffer, read from file
        // For simplicity, if the requested size is larger than buffer_size,
        // read directly without buffering
        if size >= self.buffer_size {
            return self.inner.read_at(offset, size);
        }
        
        // Read a buffer-sized chunk starting from the requested offset
        let read_size = self.buffer_size.min(self.inner.size() - offset);
        self.buffer = self.inner.read_at(offset, read_size)?;
        self.buffer_offset = offset;
        
        // Return the requested slice
        let end = size.min(self.buffer.len());
        Ok(self.buffer.slice(0..end))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file::FileSystemRegistry;

    static TEST_ROOT: &str = "file:///tmp/buffered_test";

    fn cleanup_test_root() {
        let _ = std::fs::remove_dir_all("/tmp/buffered_test");
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_buffered_writer() {
        cleanup_test_root();
        let registry = FileSystemRegistry::new();
        let fs = registry
            .get_or_register(TEST_ROOT.to_string())
            .unwrap();

        // Test writing with buffer
        {
            let writer = fs.open_write("test_buffered_write.txt").unwrap();
            let mut buffered = BufferedWriter::new(writer, 10); // Small buffer for testing

            // Write data smaller than buffer
            buffered.write(b"Hello").unwrap();
            assert_eq!(buffered.offset(), 5);

            // Write data that fills buffer and causes flush
            buffered.write(b" World!").unwrap();
            assert_eq!(buffered.offset(), 12);

            // Write more data
            buffered.write(b" Test").unwrap();
            assert_eq!(buffered.offset(), 17);

            buffered.close().unwrap();
        }

        // Verify written data
        {
            let reader = fs.open_read("test_buffered_write.txt").unwrap();
            let data = reader.read_at(0, 17).unwrap();
            assert_eq!(&data[..], b"Hello World! Test");
        }

        cleanup_test_root();
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_buffered_reader() {
        cleanup_test_root();
        let registry = FileSystemRegistry::new();
        let fs = registry
            .get_or_register(TEST_ROOT.to_string())
            .unwrap();

        // Write test data
        {
            let mut writer = fs.open_write("test_buffered_read.txt").unwrap();
            writer.write(b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz").unwrap();
            writer.close().unwrap();
        }

        // Test buffered reading
        {
            let reader = fs.open_read("test_buffered_read.txt").unwrap();
            let mut buffered = BufferedReader::new(reader, 20); // Buffer size of 20

            // First read - should fill buffer
            let data1 = buffered.read_at(0, 10).unwrap();
            assert_eq!(&data1[..], b"0123456789");

            // Second read within buffer
            let data2 = buffered.read_at(5, 10).unwrap();
            assert_eq!(&data2[..], b"56789ABCDE");

            // Third read - overlapping buffer boundary
            let data3 = buffered.read_at(15, 10).unwrap();
            assert_eq!(&data3[..], b"FGHIJKLMNO");

            // Fourth read - beyond buffer
            let data4 = buffered.read_at(40, 10).unwrap();
            assert_eq!(&data4[..], b"efghijklmn");
        }

        cleanup_test_root();
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_buffered_writer_flush() {
        cleanup_test_root();
        let registry = FileSystemRegistry::new();
        let fs = registry
            .get_or_register(TEST_ROOT.to_string())
            .unwrap();

        {
            let writer = fs.open_write("test_flush.txt").unwrap();
            let mut buffered = BufferedWriter::new(writer, 100);

            // Write data and manually flush
            buffered.write(b"Test data 1").unwrap();
            buffered.flush().unwrap();

            // Write more data
            buffered.write(b" and data 2").unwrap();
            buffered.close().unwrap();
        }

        // Verify all data was written
        {
            let reader = fs.open_read("test_flush.txt").unwrap();
            let data = reader.read_at(0, 22).unwrap();
            assert_eq!(&data[..], b"Test data 1 and data 2");
        }

        cleanup_test_root();
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_buffered_reader_large_read() {
        cleanup_test_root();
        let registry = FileSystemRegistry::new();
        let fs = registry
            .get_or_register(TEST_ROOT.to_string())
            .unwrap();

        // Write test data
        {
            let mut writer = fs.open_write("test_large_read.txt").unwrap();
            let large_data = vec![b'X'; 1000];
            writer.write(&large_data).unwrap();
            writer.close().unwrap();
        }

        // Test reading larger than buffer size
        {
            let reader = fs.open_read("test_large_read.txt").unwrap();
            let mut buffered = BufferedReader::new(reader, 100);

            // Read larger than buffer - should bypass buffer
            let data = buffered.read_at(0, 500).unwrap();
            assert_eq!(data.len(), 500);
            assert_eq!(data[0], b'X');
            assert_eq!(data[499], b'X');
        }

        cleanup_test_root();
    }
}
