//! A value log implementation for storing large values outside the main index.
//! The value log is designed to be simple and efficient for appending and reading values by pointer.
use crate::error::{Error, Result};
use crate::file::{BufferedWriter, File, RandomAccessFile, SequentialWriteFile};
use crate::file::{FileManager, TrackedFileId};
use bytes::{Buf, Bytes, BytesMut};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

const VLOG_RECORD_HEADER_SIZE: usize = 4;
const VLOG_READ_AHEAD_BYTES: usize = 1024;
const VLOG_POINTER_SIZE: usize = 8;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct VlogPointer {
    file_seq: u32,
    offset: u32,
}

impl VlogPointer {
    pub(crate) fn new(file_seq: VlogFileSeq, offset: u32) -> Self {
        Self { file_seq, offset }
    }

    pub(crate) fn file_seq(self) -> VlogFileSeq {
        self.file_seq
    }

    pub(crate) fn offset(self) -> u32 {
        self.offset
    }

    pub(crate) fn to_bytes(self) -> [u8; VLOG_POINTER_SIZE] {
        let mut bytes = [0u8; VLOG_POINTER_SIZE];
        bytes[0..4].copy_from_slice(&self.file_seq.to_le_bytes());
        bytes[4..].copy_from_slice(&self.offset.to_le_bytes());
        bytes
    }

    pub(crate) fn from_bytes(data: &[u8]) -> Result<Self> {
        if data.len() != VLOG_POINTER_SIZE {
            return Err(Error::IoError(format!(
                "Invalid VLOG pointer size: expected {}, got {}",
                VLOG_POINTER_SIZE,
                data.len()
            )));
        }
        let mut file_seq = [0u8; 4];
        file_seq.copy_from_slice(&data[..4]);
        let mut offset = [0u8; 4];
        offset.copy_from_slice(&data[4..]);
        Ok(Self {
            file_seq: u32::from_le_bytes(file_seq),
            offset: u32::from_le_bytes(offset),
        })
    }
}

pub(crate) type VlogFileSeq = u32;

/// Represents the current version of the value log, tracking the mapping of file sequences to
/// tracked file ids.
#[derive(Clone, Default)]
pub(crate) struct VlogVersion {
    file_ids: HashMap<VlogFileSeq, Arc<TrackedFileId>>,
}

impl VlogVersion {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn from_files(file_ids: Vec<(VlogFileSeq, Arc<TrackedFileId>)>) -> Self {
        let file_ids = file_ids.into_iter().collect();
        Self { file_ids }
    }

    pub(crate) fn files(&self) -> Vec<(VlogFileSeq, Arc<TrackedFileId>)> {
        self.file_ids
            .iter()
            .map(|(seq, tracked_id)| (*seq, Arc::clone(tracked_id)))
            .collect()
    }

    pub(crate) fn apply_edit(&self, edit: VlogEdit) -> Self {
        let mut file_ids = self.file_ids.clone();
        for file_seq in edit.removed_files {
            file_ids.remove(&file_seq);
        }
        for (file_seq, tracked_id) in edit.new_files {
            file_ids.insert(file_seq, tracked_id);
        }
        Self { file_ids }
    }

    fn file_id(&self, file_seq: VlogFileSeq) -> Option<Arc<TrackedFileId>> {
        self.file_ids.get(&file_seq).map(Arc::clone)
    }
}

/// Represents an edit to the value log version, including new files added and old files removed.
#[derive(Clone, Default)]
pub(crate) struct VlogEdit {
    new_files: Vec<(VlogFileSeq, Arc<TrackedFileId>)>,
    removed_files: Vec<VlogFileSeq>,
}

/// A simple value log writer that appends values to a file and returns pointers for retrieval.
pub(crate) struct VlogWriter<W: SequentialWriteFile> {
    file_seq: u32,
    writer: BufferedWriter<W>,
}

impl<W: SequentialWriteFile> VlogWriter<W> {
    fn new(file_seq: VlogFileSeq, writer: W, buffer_size: usize) -> Self {
        Self {
            file_seq,
            writer: BufferedWriter::new(writer, buffer_size),
        }
    }

    fn file_seq(&self) -> VlogFileSeq {
        self.file_seq
    }

    fn offset(&self) -> Result<u32> {
        u32::try_from(self.writer.offset())
            .map_err(|_| Error::IoError("VLOG offset overflow".to_string()))
    }

    pub(crate) fn add_value(&mut self, value: &[u8]) -> Result<VlogPointer> {
        let value_len = u32::try_from(value.len())
            .map_err(|_| Error::IoError(format!("VLOG value too large: {} bytes", value.len())))?;
        let offset = self.offset()?;
        self.writer.write(&value_len.to_le_bytes())?;
        self.writer.write(value)?;
        Ok(VlogPointer {
            file_seq: self.file_seq,
            offset,
        })
    }

    pub(crate) fn close(&mut self) -> Result<(), Error> {
        self.writer.close()
    }
}

/// A simple value log reader that reads values from a file using pointers.
pub(crate) struct VlogReader<R: RandomAccessFile> {
    reader: R,
}

impl<R: RandomAccessFile> VlogReader<R> {
    fn new(reader: R) -> Self {
        Self { reader }
    }

    fn read_value(&self, offset: u32) -> Result<Bytes> {
        let offset = offset as usize;
        let file_size = self.reader.size();
        if offset >= file_size {
            return Err(Error::IoError(format!(
                "VLOG offset {} out of range {}",
                offset, file_size
            )));
        }
        let read_size = (file_size - offset).min(VLOG_READ_AHEAD_BYTES);
        let chunk = self.reader.read_at(offset, read_size)?;
        if chunk.len() < VLOG_RECORD_HEADER_SIZE {
            return Err(Error::IoError(format!(
                "Invalid VLOG header size: {}",
                chunk.len()
            )));
        }
        let mut chunk_slice = chunk.as_ref();
        let value_len = chunk_slice.get_u32_le() as usize;
        let available = chunk.len().saturating_sub(VLOG_RECORD_HEADER_SIZE);
        if value_len <= available {
            let start = VLOG_RECORD_HEADER_SIZE;
            let end = start + value_len;
            return Ok(chunk.slice(start..end));
        }
        let mut buffer = BytesMut::with_capacity(value_len);
        buffer.extend_from_slice(&chunk[VLOG_RECORD_HEADER_SIZE..]);
        let remaining = value_len - available;
        let data_offset = offset
            .checked_add(VLOG_RECORD_HEADER_SIZE + available)
            .ok_or_else(|| Error::IoError("VLOG offset overflow".to_string()))?;
        let tail = self.reader.read_at(data_offset, remaining)?;
        buffer.extend_from_slice(&tail);
        Ok(buffer.freeze())
    }

    fn read_pointer(&self, pointer: VlogPointer) -> Result<Bytes> {
        self.read_value(pointer.offset)
    }
}

/// The main value log store that manages multiple value log files and provides APIs for writing and reading values.
pub(crate) struct VlogStore {
    file_manager: Arc<FileManager>,
    buffer_size: usize,
    value_separation_threshold: usize,
    next_file_seq: AtomicU32,
}

impl VlogStore {
    pub(crate) fn new(
        file_manager: Arc<FileManager>,
        buffer_size: usize,
        value_separation_threshold: usize,
    ) -> Self {
        Self::with_start_seq(file_manager, buffer_size, value_separation_threshold, 0)
    }

    fn with_start_seq(
        file_manager: Arc<FileManager>,
        buffer_size: usize,
        value_separation_threshold: usize,
        start_seq: VlogFileSeq,
    ) -> Self {
        Self {
            file_manager,
            buffer_size,
            value_separation_threshold,
            next_file_seq: AtomicU32::new(start_seq),
        }
    }

    pub(crate) fn allocate_file_seq(&self) -> VlogFileSeq {
        self.next_file_seq.fetch_add(1, Ordering::SeqCst)
    }

    pub(crate) fn should_separate(&self, value_len: usize) -> bool {
        value_len > self.value_separation_threshold
    }

    pub(crate) fn create_writer(
        &self,
    ) -> Result<(VlogWriter<Box<dyn SequentialWriteFile>>, VlogEdit)> {
        let file_seq = self.allocate_file_seq();
        self.create_writer_for_seq(file_seq)
    }

    pub(crate) fn create_writer_for_seq(
        &self,
        file_seq: VlogFileSeq,
    ) -> Result<(VlogWriter<Box<dyn SequentialWriteFile>>, VlogEdit)> {
        let (file_id, writer) = self.file_manager.create_data_file()?;
        let tracked_id = TrackedFileId::new(&self.file_manager, file_id);
        let writer: Box<dyn SequentialWriteFile> = Box::new(writer);
        let edit = VlogEdit {
            new_files: vec![(file_seq, tracked_id)],
            removed_files: Vec::new(),
        };
        Ok((VlogWriter::new(file_seq, writer, self.buffer_size), edit))
    }

    pub(crate) fn read_pointer(
        &self,
        version: &VlogVersion,
        pointer: VlogPointer,
    ) -> Result<Bytes> {
        let tracked_id = version.file_id(pointer.file_seq).ok_or_else(|| {
            Error::IoError(format!(
                "VLOG file seq {} is not registered",
                pointer.file_seq
            ))
        })?;
        let reader = self
            .file_manager
            .open_data_file_reader(tracked_id.file_id())?;
        let vlog_reader = VlogReader::new(reader);
        vlog_reader.read_value(pointer.offset)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file::FileManager;
    use crate::file::FileSystemRegistry;
    use crate::metrics_manager::MetricsManager;
    use std::sync::Arc;

    static TEST_ROOT: &str = "file:///tmp/vlog_test";

    fn cleanup_test_root() {
        let _ = std::fs::remove_dir_all("/tmp/vlog_test");
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_vlog_writer_reader() {
        cleanup_test_root();
        let registry = FileSystemRegistry::new();
        let fs = registry.get_or_register(TEST_ROOT.to_string()).unwrap();
        let metrics_manager = Arc::new(MetricsManager::new("vlog-test".to_string()));
        let file_manager = Arc::new(FileManager::with_defaults(fs, metrics_manager).unwrap());
        let store = VlogStore::new(Arc::clone(&file_manager), 64, usize::MAX);
        let version = VlogVersion::new();
        let (mut vlog, edit) = store.create_writer().unwrap();
        let version = version.apply_edit(edit);
        let first = vlog.add_value(b"hello").unwrap();
        let second = vlog.add_value(b"world!").unwrap();
        let large = vec![b'a'; 2000];
        let third = vlog.add_value(&large).unwrap();
        assert_eq!(first.offset, 0);
        assert_eq!(second.offset, (VLOG_RECORD_HEADER_SIZE + 5) as u32);
        vlog.close().unwrap();

        let first_value = store.read_pointer(&version, first).unwrap();
        let second_value = store.read_pointer(&version, second).unwrap();
        let third_value = store.read_pointer(&version, third).unwrap();
        assert_eq!(&first_value[..], b"hello");
        assert_eq!(&second_value[..], b"world!");
        assert_eq!(&third_value[..], &large[..]);
        cleanup_test_root();
    }

    #[test]
    fn test_vlog_file_seq_wraps() {
        let registry = FileSystemRegistry::new();
        let fs = registry.get_or_register(TEST_ROOT.to_string()).unwrap();
        let metrics_manager = Arc::new(MetricsManager::new("vlog-test".to_string()));
        let file_manager = Arc::new(FileManager::with_defaults(fs, metrics_manager).unwrap());
        let store = VlogStore::with_start_seq(Arc::clone(&file_manager), 64, usize::MAX, u32::MAX);
        let version = VlogVersion::new();
        let (mut vlog, edit) = store.create_writer().unwrap();
        assert_eq!(vlog.file_seq(), u32::MAX);
        vlog.close().unwrap();
        let version = version.apply_edit(edit);
        let (mut vlog, edit) = store.create_writer().unwrap();
        assert_eq!(vlog.file_seq(), 0);
        vlog.close().unwrap();
        let _version = version.apply_edit(edit);
    }

    #[test]
    fn test_should_separate() {
        let registry = FileSystemRegistry::new();
        let fs = registry.get_or_register(TEST_ROOT.to_string()).unwrap();
        let metrics_manager = Arc::new(MetricsManager::new("vlog-test".to_string()));
        let file_manager = Arc::new(FileManager::with_defaults(fs, metrics_manager).unwrap());
        let store = VlogStore::new(Arc::clone(&file_manager), 64, 8);
        assert!(!store.should_separate(8));
        assert!(store.should_separate(9));
    }
}
