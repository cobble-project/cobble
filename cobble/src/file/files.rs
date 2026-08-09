use crate::error::Error;
use bytes::{Bytes, BytesMut};
use std::sync::OnceLock;
use std::sync::mpsc::{Receiver, sync_channel};
use std::sync::{Arc, Mutex};
use tokio::runtime::{Builder, Handle, Runtime};
use tokio::task::{AbortHandle, JoinHandle};

pub(crate) fn read_ahead_runtime() -> Handle {
    static RUNTIME: OnceLock<Runtime> = OnceLock::new();
    RUNTIME
        .get_or_init(|| {
            Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .thread_name("cobble-read-ahead")
                .build()
                .expect("create Cobble read-ahead runtime")
        })
        .handle()
        .clone()
}

pub trait File {
    fn close(&mut self) -> Result<(), Error>;

    /// Get the size of the file in bytes
    fn size(&self) -> usize;
}

pub trait RandomAccessFile: File + Send + Sync + 'static {
    fn read_at(&self, offset: usize, size: usize) -> Result<Bytes, Error>;

    /// Whether sequential read-ahead is useful for this file's backing storage.
    fn prefers_read_ahead(&self) -> bool {
        false
    }

    /// Asynchronously read a chunk of data at the specified offset and size.
    /// Returns a JoinHandle that resolves to the read data or an error.
    /// This allows for prefetching data in the background while processing other tasks.
    fn read_at_async(
        self: Arc<Self>,
        offset: usize,
        size: usize,
        runtime: &Handle,
    ) -> JoinHandle<Result<Bytes, Error>> {
        runtime.spawn_blocking(move || self.read_at(offset, size))
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

    fn prefers_read_ahead(&self) -> bool {
        (**self).prefers_read_ahead()
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
    result: Receiver<Result<Bytes, Error>>,
    relay: JoinHandle<()>,
    io_abort: AbortHandle,
}

impl PrefetchState {
    fn abort(self) {
        self.io_abort.abort();
        self.relay.abort();
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
    runtime: Handle,
    buffer_size: usize,
    file_size: usize,
    state: Mutex<ReadAheadState>,
}

impl<R: RandomAccessFile> ReadAheadBufferedReader<R> {
    pub fn new(inner: R, buffer_size: usize, runtime: Handle) -> Self {
        let file_size = inner.size();
        Self {
            inner: Arc::new(inner),
            runtime,
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
        prefetch.result.recv().unwrap_or_else(|_| Ok(Bytes::new()))
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
            && !existing.relay.is_finished()
        {
            return;
        }
        let size = self.buffer_size.min(self.file_size.saturating_sub(offset));
        if size == 0 {
            return;
        }
        let io = Arc::clone(&self.inner).read_at_async(offset, size, &self.runtime);
        let io_abort = io.abort_handle();
        let (sender, result) = sync_channel(1);
        let relay = self.runtime.spawn(async move {
            let result = match io.await {
                Ok(result) => result,
                Err(err) if err.is_cancelled() => Ok(Bytes::new()),
                Err(err) => Err(Error::IoError(format!("Read-ahead task failed: {}", err))),
            };
            let _ = sender.send(result);
        });
        state.prefetch = Some(PrefetchState {
            offset,
            result,
            relay,
            io_abort,
        });
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
                } else if !prefetch.relay.is_finished() {
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

    fn prefers_read_ahead(&self) -> bool {
        self.inner.prefers_read_ahead()
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
#[path = "../../tests/unit/file/files.rs"]
mod tests;
