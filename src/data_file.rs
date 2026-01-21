use crate::file::FileId;

#[derive(Clone, Copy)]
pub(crate) enum DataFileType {
    SSTable,
}

pub(crate) struct DataFile {
    pub(crate) file_type: DataFileType,
    pub(crate) start_key: Vec<u8>,
    pub(crate) end_key: Vec<u8>,
    /// Unique file identifier assigned by the FileManager.
    pub(crate) file_id: FileId,
    /// Size of the file in bytes.
    pub(crate) size: usize,
}
