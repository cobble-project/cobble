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
        runtime: &Handle,
    ) -> JoinHandle<Result<Bytes, Error>> {
        let gate = Arc::clone(&self.prefetch_gate);
        let stopped = Arc::clone(&self.pending_prefetch_stopped);
        runtime.spawn(async move {
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

struct ReadAheadCapabilityFile {
    data: Bytes,
}

impl File for ReadAheadCapabilityFile {
    fn close(&mut self) -> Result<(), Error> {
        Ok(())
    }

    fn size(&self) -> usize {
        self.data.len()
    }
}

impl RandomAccessFile for ReadAheadCapabilityFile {
    fn prefers_read_ahead(&self) -> bool {
        true
    }

    fn read_at(&self, offset: usize, size: usize) -> Result<Bytes, Error> {
        let end = offset + size.min(self.data.len().saturating_sub(offset));
        Ok(self.data.slice(offset..end))
    }
}

#[test]
fn read_ahead_capability_defaults_false_and_survives_wrappers() {
    let local = AbortTrackingRandomAccessFile {
        data: Bytes::from_static(b"local"),
        pending_prefetch_stopped: Arc::new(AtomicBool::new(false)),
        prefetch_gate: Arc::new(Notify::new()),
    };
    assert!(!local.prefers_read_ahead());

    let remote = ReadAheadCapabilityFile {
        data: Bytes::from_static(b"remote"),
    };
    let remote: Box<dyn RandomAccessFile> = Box::new(remote);
    assert!(remote.prefers_read_ahead());

    let wrapped = ReadAheadBufferedReader::new(
        ReadAheadCapabilityFile {
            data: Bytes::from_static(b"remote"),
        },
        4,
        read_ahead_runtime(),
    );
    assert!(wrapped.prefers_read_ahead());
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
    let gate = Arc::new(Notify::new());
    let file = AbortTrackingRandomAccessFile {
        data: Bytes::from_static(b"0123456789"),
        pending_prefetch_stopped: Arc::new(AtomicBool::new(false)),
        prefetch_gate: Arc::clone(&gate),
    };
    let mut reader = ReadAheadBufferedReader::new(file, 4, read_ahead_runtime());

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

#[test]
fn test_read_ahead_cancelled_prefetch_falls_back_to_sync_read() {
    let gate = Arc::new(Notify::new());
    let file = AbortTrackingRandomAccessFile {
        data: Bytes::from_static(b"0123456789"),
        pending_prefetch_stopped: Arc::new(AtomicBool::new(false)),
        prefetch_gate: Arc::clone(&gate),
    };
    let reader = ReadAheadBufferedReader::new(file, 4, read_ahead_runtime());

    assert_eq!(&reader.read_at(0, 2).unwrap()[..], b"01");
    reader.cancel_prefetch().unwrap();
    assert!(
        reader.state.lock().unwrap().prefetch.is_none(),
        "explicit cancellation should clear the in-flight prefetch handle"
    );
    assert_eq!(&reader.read_at(4, 2).unwrap()[..], b"45");
}

#[test]
fn test_read_ahead_from_tokio_context() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let gate = Arc::new(Notify::new());
        let file = AbortTrackingRandomAccessFile {
            data: Bytes::from_static(b"0123456789"),
            pending_prefetch_stopped: Arc::new(AtomicBool::new(false)),
            prefetch_gate: Arc::clone(&gate),
        };
        let reader = ReadAheadBufferedReader::new(file, 4, read_ahead_runtime());

        assert_eq!(&reader.read_at(0, 2).unwrap()[..], b"01");
        gate.notify_one();
        assert_eq!(&reader.read_at(4, 2).unwrap()[..], b"45");
        gate.notify_one();
        assert_eq!(&reader.read_at(8, 2).unwrap()[..], b"89");
    });
}

struct ShortReader {
    data: Bytes,
    limit: usize,
}

impl File for ShortReader {
    fn close(&mut self) -> Result<(), Error> {
        Ok(())
    }
    fn size(&self) -> usize {
        self.data.len()
    }
}

impl RandomAccessFile for ShortReader {
    fn read_at(&self, offset: usize, size: usize) -> Result<Bytes, Error> {
        assert!(size <= COPY_CHUNK_BYTES);
        Ok(self.data.slice(offset..offset + size.min(self.limit)))
    }
}

#[derive(Default)]
struct ShortWriter {
    bytes: Vec<u8>,
    limit: usize,
    fail_after: Option<usize>,
    fail_close: bool,
    close_calls: usize,
}

impl File for ShortWriter {
    fn close(&mut self) -> Result<(), Error> {
        self.close_calls += 1;
        if self.fail_close {
            Err(Error::IoError("injected close error".into()))
        } else {
            Ok(())
        }
    }
    fn size(&self) -> usize {
        self.bytes.len()
    }
}

impl SequentialWriteFile for ShortWriter {
    fn write(&mut self, data: &[u8]) -> Result<usize, Error> {
        if self
            .fail_after
            .is_some_and(|limit| self.bytes.len() >= limit)
        {
            return Err(Error::IoError("injected write error".into()));
        }
        let count = data.len().min(self.limit).min(
            self.fail_after
                .map_or(usize::MAX, |limit| limit - self.bytes.len()),
        );
        self.bytes.extend_from_slice(&data[..count]);
        Ok(count)
    }
}

#[test]
fn copy_file_handles_short_reads_writes_and_actual_progress() {
    let source = ShortReader {
        data: Bytes::from_static(b"0123456789"),
        limit: 3,
    };
    let mut writer = ShortWriter {
        limit: 2,
        ..Default::default()
    };
    let mut progress = Vec::new();
    copy_file_contents_and_close(
        &source,
        &mut writer,
        || Ok(()),
        |count| progress.push(count),
    )
    .unwrap();
    assert_eq!(writer.bytes, source.data);
    assert_eq!(progress, [2, 1, 2, 1, 2, 1, 1]);
    assert_eq!(writer.close_calls, 1);
}

#[test]
fn copy_file_zero_progress_preserves_primary_error_and_closes() {
    for (read_limit, write_limit) in [(0, 2), (2, 0)] {
        let source = ShortReader {
            data: Bytes::from_static(b"data"),
            limit: read_limit,
        };
        let mut writer = ShortWriter {
            limit: write_limit,
            fail_close: true,
            ..Default::default()
        };
        let error =
            copy_file_contents_and_close(&source, &mut writer, || Ok(()), |_| {}).unwrap_err();
        assert!(error.to_string().contains("invalid byte count"));
        assert_eq!(writer.close_calls, 1);
    }
}

#[test]
fn copy_file_checks_cancellation_between_partial_writes_and_after_close() {
    let source = ShortReader {
        data: Bytes::from_static(b"data"),
        limit: 4,
    };
    let mut writer = ShortWriter {
        limit: 2,
        ..Default::default()
    };
    let cancelled = AtomicBool::new(false);
    let error = copy_file_contents_and_close(
        &source,
        &mut writer,
        || {
            if cancelled.load(Ordering::SeqCst) {
                Err(Error::CancelledError("cancelled".into()))
            } else {
                Ok(())
            }
        },
        |_| cancelled.store(true, Ordering::SeqCst),
    )
    .unwrap_err();
    assert!(matches!(error, Error::CancelledError(_)));
    assert_eq!(writer.bytes, b"da");
    assert_eq!(writer.close_calls, 1);

    let empty = ShortReader {
        data: Bytes::new(),
        limit: 1,
    };
    let mut checks = 0;
    let error = copy_file_contents_and_close(
        &empty,
        &mut writer,
        || {
            checks += 1;
            if checks == 2 {
                Err(Error::CancelledError("cancelled after close".into()))
            } else {
                Ok(())
            }
        },
        |_| {},
    )
    .unwrap_err();
    assert!(matches!(error, Error::CancelledError(_)));
    assert_eq!(writer.close_calls, 2);
}

#[test]
fn buffered_writer_partial_failure_retry_does_not_duplicate_written_prefix() {
    let inner = ShortWriter {
        limit: 2,
        fail_after: Some(3),
        ..Default::default()
    };
    let mut writer = BufferedWriter::new(inner, 8);
    writer.write(b"abcdef").unwrap();
    assert!(writer.flush().is_err());
    assert_eq!(writer.inner.bytes, b"abc");
    assert_eq!(writer.offset(), 6);
    writer.inner.fail_after = None;
    writer.close().unwrap();
    assert_eq!(writer.inner.bytes, b"abcdef");
    assert_eq!(writer.inner.close_calls, 1);
}

#[test]
fn buffered_writer_full_flush_retains_buffer_capacity() {
    let inner = ShortWriter {
        limit: 2,
        ..Default::default()
    };
    let mut writer = BufferedWriter::new(inner, 8);
    let capacity = writer.buffer.capacity();
    for _ in 0..2 {
        writer.write(b"abcdefgh").unwrap();
        writer.flush().unwrap();
        assert!(writer.buffer.is_empty());
        assert_eq!(writer.buffer.capacity(), capacity);
    }
    assert_eq!(writer.inner.bytes, b"abcdefghabcdefgh");
    writer.close().unwrap();
}

#[test]
fn buffered_writer_closes_after_flush_failure_and_preserves_primary_error() {
    let inner = ShortWriter {
        limit: 2,
        fail_after: Some(3),
        fail_close: true,
        ..Default::default()
    };
    let mut writer = BufferedWriter::new(inner, 8);
    writer.write(b"abcdef").unwrap();
    assert!(
        writer
            .close()
            .unwrap_err()
            .to_string()
            .contains("injected write error")
    );
    assert_eq!(writer.inner.close_calls, 1);
    assert_eq!(writer.inner.bytes, b"abc");
    assert!(writer.close().is_err());
    assert_eq!(writer.inner.bytes, b"abc");
}
