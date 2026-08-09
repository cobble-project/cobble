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
