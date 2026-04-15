use crate::error::{Error, Result};
use crate::file::files::{File, RandomAccessFile, SequentialWriteFile};
use bytes::{Buf, Bytes};
use std::sync::Arc;
use ::opendal::Buffer;

pub(crate) struct OpendalRandomAccessFile {
    pub(crate) size: usize,
    pub(crate) reader: opendal::Reader,
    pub(crate) runtime: Arc<tokio::runtime::Runtime>,
}

impl File for OpendalRandomAccessFile {
    fn close(&mut self) -> Result<()> {
        todo!()
    }

    fn size(&self) -> usize {
        self.size
    }
}

impl RandomAccessFile for OpendalRandomAccessFile {
    fn read_at(&self, offset: usize, size: usize) -> Result<Bytes> {
        self.runtime
            .block_on(async {
                let result: opendal::Result<Buffer> =
                    self.reader.read(offset as u64..(offset + size) as u64).await;
                result.map(|data: Buffer| data.to_bytes())
            })
            .map_err(|e| {
                Error::IoError(format!(
                    "Failed to read at offset {} size {}: {}",
                    offset, size, e
                ))
            })
    }

    fn read_at_async(
        self: Arc<Self>,
        offset: usize,
        size: usize,
    ) -> tokio::task::JoinHandle<Result<Bytes>> {
        let reader = self.reader.clone();
        let runtime = Arc::clone(&self.runtime);
        runtime.spawn(async move {
            let result: opendal::Result<Buffer> =
                reader.read(offset as u64..(offset + size) as u64).await;
            result
                .map(|data: Buffer| data.to_bytes())
                .map_err(|e| {
                    Error::IoError(format!(
                        "Failed to read at offset {} size {}: {}",
                        offset, size, e
                    ))
                })
        })
    }
}

pub(crate) struct OpendalSequentialWriteFile {
    pub(crate) size: usize,
    pub(crate) writer: opendal::Writer,
    pub(crate) runtime: Arc<tokio::runtime::Runtime>,
}

impl File for OpendalSequentialWriteFile {
    fn close(&mut self) -> Result<()> {
        self.runtime
            .block_on(async {
                let result: opendal::Result<opendal::Metadata> = self.writer.close().await;
                result.map(|_| ())
            })
            .map_err(|e| Error::IoError(format!("Failed to close writer: {}", e)))
    }

    fn size(&self) -> usize {
        self.size
    }
}

impl SequentialWriteFile for OpendalSequentialWriteFile {
    fn write(&mut self, data: &[u8]) -> Result<usize> {
        let len = data.remaining();
        self.runtime
            .block_on(async {
                let result: opendal::Result<()> = self.writer.write_from(data).await;
                result.map(|_| len)
            })
            .map_err(|e| Error::IoError(format!("Failed to write data of size {}: {}", len, e)))?;

        // Update the file size after successful write
        self.size += len;
        Ok(len)
    }
}
