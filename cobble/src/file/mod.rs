pub mod atomic_replace;
pub(crate) mod deletion;
#[cfg(test)]
#[path = "../../tests/unit/file/deletion_probe.rs"]
pub(crate) mod deletion_probe;
mod file_manager;
mod file_system;
mod files;
pub(crate) mod logical_file;
pub(crate) mod metadata_io;
mod offload;
mod opendal_file;
mod opendal_fs;
#[cfg(unix)]
mod posix_fs;
mod tracked_file;
mod volume;
#[cfg(windows)]
mod windows_fs;

pub use self::file_manager::FileManager;
pub(crate) use self::file_manager::FileManagerMetrics;
pub(crate) use self::file_manager::RestoreCopyResourceRegistry;
pub(crate) use self::file_manager::SnapshotCopyResourceRegistry;
#[cfg(test)]
pub(crate) use self::file_manager::test_utils;
pub use self::file_system::{
    FastCopyDestination, FileSystem, FileSystemRegistry, ProcessFileSystemRegistry,
    ProcessFileSystemRequest, clear_process_custom_file_system_registry,
    register_process_custom_file_system_registry,
};
pub use self::files::{
    BufferedReader, BufferedWriter, File, RandomAccessFile, ReadAheadBufferedReader,
    SequentialWriteFile,
};
pub(crate) use self::files::{ReadAllFile, read_ahead_runtime, write_all};
pub(crate) use self::metadata_io::{MetadataReader, MetadataWriter};
pub(crate) use self::offload::{
    PrimaryOffloadFileRef, PrimaryTieringWorkerHandle, compare_primary_offload_file_refs,
};
pub use self::tracked_file::{FileId, TrackedFile, TrackedFileId, TrackedWriter};
pub(crate) use self::tracked_file::{VLOG_FILE_PRIORITY, lsm_file_priority_for_level};
pub(crate) use self::volume::{DataVolume, PrimaryDataPlacement};
