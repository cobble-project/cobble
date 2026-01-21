use crate::file::FileHandle;

pub(crate) enum DataFileType {
    SSTable,
}

pub(crate) struct DataFile {
    pub(crate) file_handle: FileHandle,
    pub(crate) file_type: DataFileType,
    pub(crate) start_key: Vec<u8>,
    pub(crate) end_key: Vec<u8>,
    /// Path to the file relative to the file system root.
    pub(crate) path: String,
}
