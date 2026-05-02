use crate::error::Error;
use bytes::{Bytes, BytesMut};
use std::sync::{Arc, Mutex};
use tokio::task::JoinHandle;

pub trait File {
    fn close(&mut self) -> Result<(), Error>;

    /// Get the size of the file in bytes
    fn size(&self) -> usize;
}

pub trait RandomAccessFile: File + Send + Sync + 'static {
    fn read_at(&self, offset: usize, size: usize) -> Result<Bytes, Error>;

    /// Asynchronously read a chunk of data at the specified offset and size.
    /// Returns a JoinHandle that resolves to the read data or an error.
    /// This allows for prefetching data in the background while processing other tasks.
    fn read_at_async(
        self: Arc<Self>,
        offset: usize,
        size: usize,
    ) -> JoinHandle<Result<Bytes, Error>> {
        let handle = tokio::runtime::Handle::try_current()
            .expect("Read-ahead requires an active tokio runtime");
        handle.spawn_blocking(move || self.read_at(offset, size))
    }
}

pub trait ReadAllFile {
    fn read_all(&self) -> Result<Bytes, Error>;
}

impl<T: RandomAccessFile + ?Sized> ReadAllFile for T {
    fn read_all(&self) -> Result<Bytes, Error> {
        self.read_at(0, self.size())
    }
}

pub trait SequentialWriteFile: File + Send {
    fn write(&mut self, data: &[u8]) -> Result<usize, Error>;
}

// Implement File for Box<dyn RandomAccessFile>
impl File for Box<dyn RandomAccessFile> {
    fn close(&mut self) -> Result<(), Error> {
        (**self).close()
    }

    fn size(&self) -> usize {
        (**self).size()
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

    fn size(&self) -> usize {
        (**self).size()
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

struct PrefetchState {
    offset: usize,
    handle: JoinHandle<Result<Bytes, Error>>,
}

impl PrefetchState {
    fn abort(self) {
        self.handle.abort();
    }
}

struct ReadAheadState {
    buffer: Bytes,
    buffer_offset: usize,
    prefetch: Option<PrefetchState>,
}

/// A read-ahead buffered reader for sequential random access reads.
/// It buffers a fixed-size window and asynchronously prefetches the next window.
pub struct ReadAheadBufferedReader<R: RandomAccessFile> {
    inner: Arc<R>,
    buffer_size: usize,
    file_size: usize,
    state: Mutex<ReadAheadState>,
}

impl<R: RandomAccessFile> ReadAheadBufferedReader<R> {
    pub fn new(inner: R, buffer_size: usize) -> Self {
        let file_size = inner.size();
        Self {
            inner: Arc::new(inner),
            buffer_size,
            file_size,
            state: Mutex::new(ReadAheadState {
                buffer: Bytes::new(),
                buffer_offset: 0,
                prefetch: None,
            }),
        }
    }

    fn join_prefetch(&self, prefetch: PrefetchState) -> Result<Bytes, Error> {
        let handle = tokio::runtime::Handle::try_current()
            .map_err(|_| Error::IoError("Read-ahead requires a tokio runtime".to_string()))?;
        match handle.block_on(prefetch.handle) {
            Ok(result) => result,
            Err(err) if err.is_cancelled() => Ok(Bytes::new()),
            Err(err) => Err(Error::IoError(format!("Read-ahead task failed: {}", err))),
        }
    }

    fn cancel_prefetch(&self) -> Result<(), Error> {
        let prefetch = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| Error::IoError("Read-ahead state lock poisoned".to_string()))?;
            state.prefetch.take()
        };
        if let Some(prefetch) = prefetch {
            prefetch.abort();
        }
        Ok(())
    }

    fn schedule_prefetch(&self, state: &mut ReadAheadState, offset: usize) {
        if offset >= self.file_size || self.buffer_size == 0 {
            return;
        }
        if let Some(existing) = &state.prefetch
            && !existing.handle.is_finished()
        {
            return;
        }
        let size = self.buffer_size.min(self.file_size.saturating_sub(offset));
        if size == 0 {
            return;
        }
        let handle = Arc::clone(&self.inner).read_at_async(offset, size);
        state.prefetch = Some(PrefetchState { offset, handle });
    }

    pub fn read_at(&self, offset: usize, size: usize) -> Result<Bytes, Error> {
        if size == 0 {
            return Ok(Bytes::new());
        }
        {
            let mut state = self
                .state
                .lock()
                .map_err(|_| Error::IoError("Read-ahead state lock poisoned".to_string()))?;
            let buffer_end = state.buffer_offset + state.buffer.len();
            if offset >= state.buffer_offset && offset + size <= buffer_end {
                let start = offset - state.buffer_offset;
                return Ok(state.buffer.slice(start..start + size));
            }
            if let Some(prefetch) = state.prefetch.take() {
                if offset >= prefetch.offset && offset < prefetch.offset + self.buffer_size {
                    drop(state);
                    let prefetch_offset = prefetch.offset;
                    let buffer = self.join_prefetch(prefetch)?;
                    let mut state = self.state.lock().map_err(|_| {
                        Error::IoError("Read-ahead state lock poisoned".to_string())
                    })?;
                    state.buffer_offset = prefetch_offset;
                    state.buffer = buffer;
                    let buffer_end = state.buffer_offset + state.buffer.len();
                    if offset + size <= buffer_end {
                        let next_offset = state.buffer_offset + state.buffer.len();
                        self.schedule_prefetch(&mut state, next_offset);
                        let start = offset - state.buffer_offset;
                        return Ok(state.buffer.slice(start..start + size));
                    }
                } else if !prefetch.handle.is_finished() {
                    state.prefetch = Some(prefetch);
                }
            }
        }

        let read_size = self.buffer_size.max(size);
        let remaining = self.inner.size().saturating_sub(offset);
        let read_size = read_size.min(remaining);
        let buffer = self.inner.read_at(offset, read_size)?;

        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::IoError("Read-ahead state lock poisoned".to_string()))?;
        state.buffer_offset = offset;
        state.buffer = buffer;
        let next_offset = state.buffer_offset + state.buffer.len();
        self.schedule_prefetch(&mut state, next_offset);
        let buffer_end = state.buffer_offset + state.buffer.len();
        if offset + size <= buffer_end {
            let start = offset - state.buffer_offset;
            Ok(state.buffer.slice(start..start + size))
        } else {
            Err(Error::IoError("Read-ahead buffer underrun".to_string()))
        }
    }
}

impl<R: RandomAccessFile> File for ReadAheadBufferedReader<R> {
    fn close(&mut self) -> Result<(), Error> {
        self.cancel_prefetch()?;
        if let Some(inner) = Arc::get_mut(&mut self.inner) {
            inner.close()
        } else {
            Ok(())
        }
    }

    fn size(&self) -> usize {
        self.file_size
    }
}

impl<R: RandomAccessFile> RandomAccessFile for ReadAheadBufferedReader<R> {
    fn read_at(&self, offset: usize, size: usize) -> Result<Bytes, Error> {
        ReadAheadBufferedReader::read_at(self, offset, size)
    }
}

impl<R: RandomAccessFile> Drop for ReadAheadBufferedReader<R> {
    fn drop(&mut self) {
        let _ = self.cancel_prefetch();
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

    pub fn offset(&self) -> usize {
        self.offset + self.buffer.len()
    }
}

impl<W: SequentialWriteFile> File for BufferedWriter<W> {
    fn close(&mut self) -> Result<(), Error> {
        if !self.buffer.is_empty() {
            let data = self.buffer.split();
            let len = data.len();
            self.inner.write(&data)?;
            self.offset += len;
        }
        self.inner.close()
    }

    fn size(&self) -> usize {
        self.offset()
    }
}

impl<W: SequentialWriteFile> SequentialWriteFile for BufferedWriter<W> {
    fn write(&mut self, data: &[u8]) -> Result<usize, Error> {
        BufferedWriter::write(self, data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file::FileSystemRegistry;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::sync::Notify;

    static TEST_ROOT: &str = "file:///tmp/buffered_test";

    fn cleanup_test_root() {
        let _ = std::fs::remove_dir_all("/tmp/buffered_test");
    }

    struct AbortTrackingRandomAccessFile {
        data: Bytes,
        pending_prefetch_stopped: Arc<AtomicBool>,
        prefetch_gate: Arc<Notify>,
    }

    impl File for AbortTrackingRandomAccessFile {
        fn close(&mut self) -> Result<(), Error> {
            Ok(())
        }

        fn size(&self) -> usize {
            self.data.len()
        }
    }

    impl RandomAccessFile for AbortTrackingRandomAccessFile {
        fn read_at(&self, offset: usize, size: usize) -> Result<Bytes, Error> {
            let end = offset + size.min(self.data.len().saturating_sub(offset));
            Ok(self.data.slice(offset..end))
        }

        fn read_at_async(
            self: Arc<Self>,
            offset: usize,
            size: usize,
        ) -> JoinHandle<Result<Bytes, Error>> {
            let gate = Arc::clone(&self.prefetch_gate);
            let stopped = Arc::clone(&self.pending_prefetch_stopped);
            tokio::spawn(async move {
                struct StopGuard {
                    stopped: Arc<AtomicBool>,
                }

                impl Drop for StopGuard {
                    fn drop(&mut self) {
                        self.stopped.store(true, Ordering::SeqCst);
                    }
                }

                let _guard = StopGuard { stopped };
                gate.notified().await;
                self.read_at(offset, size)
            })
        }
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_buffered_writer() {
        cleanup_test_root();
        let registry = FileSystemRegistry::new();
        let fs = registry.get_or_register(TEST_ROOT).unwrap();

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
        let fs = registry.get_or_register(TEST_ROOT).unwrap();

        // Write test data
        {
            let mut writer = fs.open_write("test_buffered_read.txt").unwrap();
            writer
                .write(b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz")
                .unwrap();
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
        let fs = registry.get_or_register(TEST_ROOT).unwrap();

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
        let fs = registry.get_or_register(TEST_ROOT).unwrap();

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

    #[test]
    fn test_read_ahead_close_aborts_pending_prefetch() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        {
            let _guard = runtime.enter();
            let gate = Arc::new(Notify::new());
            let file = AbortTrackingRandomAccessFile {
                data: Bytes::from_static(b"0123456789"),
                pending_prefetch_stopped: Arc::new(AtomicBool::new(false)),
                prefetch_gate: Arc::clone(&gate),
            };
            let mut reader = ReadAheadBufferedReader::new(file, 4);

            assert_eq!(&reader.read_at(0, 2).unwrap()[..], b"01");
            assert!(
                reader.state.lock().unwrap().prefetch.is_some(),
                "prefetch should be scheduled after the initial read"
            );

            reader.close().unwrap();
            assert!(
                reader.state.lock().unwrap().prefetch.is_none(),
                "close should clear the in-flight prefetch handle"
            );
        }
        runtime.block_on(tokio::task::yield_now());
    }

    #[test]
    fn test_read_ahead_cancelled_prefetch_falls_back_to_sync_read() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        {
            let _guard = runtime.enter();
            let gate = Arc::new(Notify::new());
            let file = AbortTrackingRandomAccessFile {
                data: Bytes::from_static(b"0123456789"),
                pending_prefetch_stopped: Arc::new(AtomicBool::new(false)),
                prefetch_gate: Arc::clone(&gate),
            };
            let reader = ReadAheadBufferedReader::new(file, 4);

            assert_eq!(&reader.read_at(0, 2).unwrap()[..], b"01");
            reader.cancel_prefetch().unwrap();
            assert!(
                reader.state.lock().unwrap().prefetch.is_none(),
                "explicit cancellation should clear the in-flight prefetch handle"
            );
            assert_eq!(&reader.read_at(4, 2).unwrap()[..], b"45");
        }
        runtime.block_on(tokio::task::yield_now());
    }
}
