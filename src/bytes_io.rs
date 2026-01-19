use crate::error::{Error, Result};
use bytes::{Bytes, BytesMut, BufMut};
use std::io::{Read, Write};
use std::fs::{File, OpenOptions};
use std::path::Path;

/// Internal bytes buffer for managing byte data
#[derive(Clone, Debug)]
pub struct BytesBuffer {
    data: BytesMut,
}

impl BytesBuffer {
    /// Create a new empty BytesBuffer
    pub fn new() -> Self {
        Self {
            data: BytesMut::new(),
        }
    }

    /// Create a BytesBuffer with specified capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            data: BytesMut::with_capacity(capacity),
        }
    }

    /// Create a BytesBuffer from existing bytes
    pub fn from_bytes(bytes: Bytes) -> Self {
        Self {
            data: BytesMut::from(&bytes[..]),
        }
    }

    /// Get the length of the buffer
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Check if the buffer is empty
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Get a slice of the buffer
    pub fn as_slice(&self) -> &[u8] {
        &self.data[..]
    }

    /// Convert to immutable Bytes
    pub fn freeze(self) -> Bytes {
        self.data.freeze()
    }

    /// Clear the buffer
    pub fn clear(&mut self) {
        self.data.clear();
    }

    /// Write bytes to the buffer
    pub fn write(&mut self, data: &[u8]) -> usize {
        self.data.put_slice(data);
        data.len()
    }

    /// Read at a specific offset
    pub fn read_at(&self, offset: usize, size: usize) -> Result<Bytes> {
        if offset + size > self.data.len() {
            return Err(Error::IoError(format!(
                "Read out of bounds: offset={}, size={}, buffer_len={}",
                offset, size, self.data.len()
            )));
        }
        Ok(Bytes::copy_from_slice(&self.data[offset..offset + size]))
    }

    /// Write at a specific offset (overwrites existing data)
    pub fn write_at(&mut self, offset: usize, data: &[u8]) -> Result<usize> {
        let end = offset + data.len();
        if end > self.data.len() {
            // Extend the buffer if necessary
            self.data.resize(end, 0);
        }
        self.data[offset..end].copy_from_slice(data);
        Ok(data.len())
    }
}

impl Default for BytesBuffer {
    fn default() -> Self {
        Self::new()
    }
}

/// Reader for sequential and random access reads from bytes
pub struct BytesReader {
    data: Bytes,
    position: usize,
}

impl BytesReader {
    /// Create a new BytesReader from Bytes
    pub fn new(data: Bytes) -> Self {
        Self {
            data,
            position: 0,
        }
    }

    /// Create a BytesReader from a file
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let mut file = File::open(path)
            .map_err(|e| Error::IoError(format!("Failed to open file: {}", e)))?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)
            .map_err(|e| Error::IoError(format!("Failed to read file: {}", e)))?;
        Ok(Self::new(Bytes::from(buffer)))
    }

    /// Get the total length of the data
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Check if the reader is empty
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Get the current position
    pub fn position(&self) -> usize {
        self.position
    }

    /// Seek to a specific position
    pub fn seek(&mut self, position: usize) -> Result<()> {
        if position > self.data.len() {
            return Err(Error::IoError(format!(
                "Seek position out of bounds: position={}, len={}",
                position, self.data.len()
            )));
        }
        self.position = position;
        Ok(())
    }

    /// Read bytes sequentially from current position
    pub fn read(&mut self, size: usize) -> Result<Bytes> {
        if self.position + size > self.data.len() {
            return Err(Error::IoError(format!(
                "Read out of bounds: position={}, size={}, len={}",
                self.position, size, self.data.len()
            )));
        }
        let result = self.data.slice(self.position..self.position + size);
        self.position += size;
        Ok(result)
    }

    /// Read bytes at a specific offset (random access)
    pub fn read_at(&self, offset: usize, size: usize) -> Result<Bytes> {
        if offset + size > self.data.len() {
            return Err(Error::IoError(format!(
                "Read out of bounds: offset={}, size={}, len={}",
                offset, size, self.data.len()
            )));
        }
        Ok(self.data.slice(offset..offset + size))
    }

    /// Read all remaining bytes from current position
    pub fn read_remaining(&mut self) -> Bytes {
        let result = self.data.slice(self.position..);
        self.position = self.data.len();
        result
    }

    /// Get a reference to all data
    pub fn get_ref(&self) -> &Bytes {
        &self.data
    }
}

/// Writer for sequential and random access writes to bytes
pub struct BytesWriter {
    buffer: BytesMut,
}

impl BytesWriter {
    /// Create a new BytesWriter
    pub fn new() -> Self {
        Self {
            buffer: BytesMut::new(),
        }
    }

    /// Create a BytesWriter with specified capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            buffer: BytesMut::with_capacity(capacity),
        }
    }

    /// Get the current length of the buffer
    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    /// Check if the writer is empty
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    /// Write bytes sequentially (append)
    pub fn write(&mut self, data: &[u8]) -> Result<usize> {
        self.buffer.put_slice(data);
        Ok(data.len())
    }

    /// Write bytes at a specific offset (random access)
    pub fn write_at(&mut self, offset: usize, data: &[u8]) -> Result<usize> {
        let end = offset + data.len();
        if end > self.buffer.len() {
            // Extend the buffer if necessary
            self.buffer.resize(end, 0);
        }
        self.buffer[offset..end].copy_from_slice(data);
        Ok(data.len())
    }

    /// Get the written data as immutable Bytes
    pub fn into_bytes(self) -> Bytes {
        self.buffer.freeze()
    }

    /// Get a reference to the buffer as a slice
    pub fn as_slice(&self) -> &[u8] {
        &self.buffer[..]
    }

    /// Write the buffer to a file
    pub fn write_to_file<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .map_err(|e| Error::IoError(format!("Failed to open file for writing: {}", e)))?;
        file.write_all(&self.buffer)
            .map_err(|e| Error::IoError(format!("Failed to write to file: {}", e)))?;
        Ok(())
    }

    /// Clear the buffer
    pub fn clear(&mut self) {
        self.buffer.clear();
    }
}

impl Default for BytesWriter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_bytes_buffer_basic() {
        let mut buffer = BytesBuffer::new();
        assert_eq!(buffer.len(), 0);
        assert!(buffer.is_empty());

        let data = b"Hello, World!";
        let written = buffer.write(data);
        assert_eq!(written, data.len());
        assert_eq!(buffer.len(), data.len());
        assert!(!buffer.is_empty());
        assert_eq!(buffer.as_slice(), data);
    }

    #[test]
    fn test_bytes_buffer_read_at() {
        let mut buffer = BytesBuffer::new();
        buffer.write(b"Hello, World!");

        let result = buffer.read_at(0, 5).unwrap();
        assert_eq!(&result[..], b"Hello");

        let result = buffer.read_at(7, 5).unwrap();
        assert_eq!(&result[..], b"World");

        // Test out of bounds
        assert!(buffer.read_at(0, 100).is_err());
    }

    #[test]
    fn test_bytes_buffer_write_at() {
        let mut buffer = BytesBuffer::new();
        buffer.write(b"Hello, World!");

        // Overwrite existing data
        buffer.write_at(7, b"Rust!").unwrap();
        let result = buffer.read_at(0, buffer.len()).unwrap();
        assert_eq!(&result[..], b"Hello, Rust!!");

        // Extend buffer
        buffer.write_at(20, b"New").unwrap();
        assert!(buffer.len() >= 23);
    }

    #[test]
    fn test_bytes_buffer_freeze() {
        let mut buffer = BytesBuffer::new();
        buffer.write(b"Test data");
        let bytes = buffer.freeze();
        assert_eq!(&bytes[..], b"Test data");
    }

    #[test]
    fn test_bytes_reader_sequential() {
        let data = Bytes::from_static(b"Hello, World!");
        let mut reader = BytesReader::new(data);

        assert_eq!(reader.len(), 13);
        assert_eq!(reader.position(), 0);

        let result = reader.read(5).unwrap();
        assert_eq!(&result[..], b"Hello");
        assert_eq!(reader.position(), 5);

        let result = reader.read(2).unwrap();
        assert_eq!(&result[..], b", ");
        assert_eq!(reader.position(), 7);

        let result = reader.read_remaining();
        assert_eq!(&result[..], b"World!");
        assert_eq!(reader.position(), 13);
    }

    #[test]
    fn test_bytes_reader_random_access() {
        let data = Bytes::from_static(b"Hello, World!");
        let reader = BytesReader::new(data);

        let result = reader.read_at(0, 5).unwrap();
        assert_eq!(&result[..], b"Hello");

        let result = reader.read_at(7, 5).unwrap();
        assert_eq!(&result[..], b"World");

        // Position should not change with read_at
        assert_eq!(reader.position(), 0);

        // Test out of bounds
        assert!(reader.read_at(0, 100).is_err());
    }

    #[test]
    fn test_bytes_reader_seek() {
        let data = Bytes::from_static(b"Hello, World!");
        let mut reader = BytesReader::new(data);

        reader.seek(7).unwrap();
        assert_eq!(reader.position(), 7);

        let result = reader.read(5).unwrap();
        assert_eq!(&result[..], b"World");

        // Test out of bounds seek
        assert!(reader.seek(100).is_err());
    }

    #[test]
    fn test_bytes_writer_sequential() {
        let mut writer = BytesWriter::new();

        writer.write(b"Hello").unwrap();
        writer.write(b", ").unwrap();
        writer.write(b"World!").unwrap();

        assert_eq!(writer.len(), 13);
        assert_eq!(writer.as_slice(), b"Hello, World!");

        let bytes = writer.into_bytes();
        assert_eq!(&bytes[..], b"Hello, World!");
    }

    #[test]
    fn test_bytes_writer_random_access() {
        let mut writer = BytesWriter::with_capacity(20);

        // Write at different positions
        writer.write_at(0, b"Hello").unwrap();
        writer.write_at(7, b"World").unwrap();

        // Fill the gap
        writer.write_at(5, b", ").unwrap();

        let result = writer.as_slice();
        assert_eq!(&result[..12], b"Hello, World");
    }

    #[test]
    fn test_bytes_writer_overwrite() {
        let mut writer = BytesWriter::new();
        writer.write(b"Hello, World!").unwrap();

        // Overwrite part of the data
        writer.write_at(7, b"Rust!").unwrap();

        assert_eq!(writer.as_slice(), b"Hello, Rust!!");
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_bytes_file_operations() {
        let test_path = std::env::temp_dir().join("test_bytes_io.txt");
        let _ = fs::remove_file(&test_path);

        // Write to file
        let mut writer = BytesWriter::new();
        writer.write(b"Hello, File System!").unwrap();
        writer.write_to_file(&test_path).unwrap();

        // Read from file
        let reader = BytesReader::from_file(&test_path).unwrap();
        assert_eq!(reader.len(), 19);
        assert_eq!(reader.get_ref(), &Bytes::from_static(b"Hello, File System!"));

        // Clean up
        let _ = fs::remove_file(&test_path);
    }

    #[test]
    fn test_bytes_buffer_with_capacity() {
        let buffer = BytesBuffer::with_capacity(100);
        assert_eq!(buffer.len(), 0);
        assert!(buffer.is_empty());
    }

    #[test]
    fn test_bytes_buffer_clear() {
        let mut buffer = BytesBuffer::new();
        buffer.write(b"Test");
        assert!(!buffer.is_empty());
        buffer.clear();
        assert!(buffer.is_empty());
        assert_eq!(buffer.len(), 0);
    }

    #[test]
    fn test_bytes_writer_clear() {
        let mut writer = BytesWriter::new();
        writer.write(b"Test").unwrap();
        assert!(!writer.is_empty());
        writer.clear();
        assert!(writer.is_empty());
        assert_eq!(writer.len(), 0);
    }

    #[test]
    fn test_bytes_buffer_from_bytes() {
        let original = Bytes::from_static(b"Test data");
        let buffer = BytesBuffer::from_bytes(original.clone());
        assert_eq!(buffer.len(), original.len());
        assert_eq!(buffer.as_slice(), &original[..]);
    }
}
