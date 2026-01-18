use bytes::Bytes;
use crate::error::Error;

pub struct FileHandle {
    pub id: u64,
}

pub trait File {
    fn close(&mut self) -> Result<(), Error>;
    fn get_handle(&self) -> &FileHandle;
}

pub trait RandomAccessFile : File {
    fn read_at(&self, offset: usize, size: usize) -> Result<Bytes, Error>;
}

pub trait SequentialWriteFile : File {
    fn write(&mut self, data: &[u8]) -> Result<usize, Error>;
}