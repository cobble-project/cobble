use crate::file::FileId;
use std::fmt;
use std::str::FromStr;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DataFileType {
    SSTable,
}

impl DataFileType {
    pub fn as_str(self) -> &'static str {
        match self {
            DataFileType::SSTable => "sst",
        }
    }
}

impl fmt::Display for DataFileType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for DataFileType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "sst" => Ok(DataFileType::SSTable),
            _ => Err(format!("Unknown data file type: {}", s)),
        }
    }
}

pub struct DataFile {
    pub file_type: DataFileType,
    pub start_key: Vec<u8>,
    pub end_key: Vec<u8>,
    /// Unique file identifier assigned by the FileManager.
    pub file_id: FileId,
    /// Maximum sequence id for data contained in this file.
    pub seq: u64,
    /// Size of the file in bytes.
    pub size: usize,
}
