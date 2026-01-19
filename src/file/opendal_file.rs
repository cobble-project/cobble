use crate::error::{Error, Result};
use crate::file::FileHandle;
use crate::file::files::{File, RandomAccessFile, SequentialWriteFile};
use bytes::{Buf, Bytes};
use std::sync::Arc;

pub(crate) struct OpendalRandomAccessFile {
    pub(crate) handle: FileHandle,
    pub(crate) reader: opendal::Reader,
    pub(crate) runtime: Arc<tokio::runtime::Runtime>,
}

impl File for OpendalRandomAccessFile {
    fn close(&mut self) -> Result<()> {
        todo!()
    }

    fn get_handle(&self) -> &FileHandle {
        &self.handle
    }
}

impl RandomAccessFile for OpendalRandomAccessFile {
    fn read_at(&self, offset: usize, size: usize) -> Result<Bytes> {
        self.runtime
            .block_on(async {
                self.reader
                    .read(offset as u64..(offset + size) as u64)
                    .await
                    .map(|data| data.to_bytes())
            })
            .map_err(|e| {
                Error::IoError(format!(
                    "Failed to read at offset {} size {}: {}",
                    offset, size, e
                ))
            })
    }
}

pub(crate) struct OpendalSequentialWriteFile {
    pub(crate) handle: FileHandle,
    pub(crate) writer: opendal::Writer,
    pub(crate) runtime: Arc<tokio::runtime::Runtime>,
}

impl File for OpendalSequentialWriteFile {
    fn close(&mut self) -> Result<()> {
        self.runtime
            .block_on(async { self.writer.close().await.map(|_| ()) })
            .map_err(|e| Error::IoError(format!("Failed to close writer: {}", e)))
    }

    fn get_handle(&self) -> &FileHandle {
        &self.handle
    }
}

impl SequentialWriteFile for OpendalSequentialWriteFile {
    fn write(&mut self, data: &[u8]) -> Result<usize> {
        let len = data.remaining();
        self.runtime
            .block_on(async { self.writer.write_from(data).await.map(|_| len) })
            .map_err(|e| {
                Error::IoError(format!(
                    "Failed to write data of size {}: {}",
                    len,
                    e
                ))
            })?;
        
        // Update the file size after successful write
        self.handle.size += len;
        Ok(len)
    }
}
