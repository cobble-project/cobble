use super::*;
use crate::file::{FastCopyDestination, FileSystem};
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;

static TEST_ROOT: &str = "file:///tmp/posix_fs_test";

#[test]
fn concurrent_delete_is_idempotent_but_preserves_other_errors() {
    let root = tempfile::tempdir().unwrap();
    let fs = PosixFileSystem::init(
        &Url::from_directory_path(root.path()).unwrap(),
        None,
        None,
        None,
    )
    .unwrap();
    for _ in 0..64 {
        fs.open_write("retired").unwrap().close().unwrap();
        let start = std::sync::Barrier::new(2);
        std::thread::scope(|scope| {
            let a = scope.spawn(|| {
                start.wait();
                fs.delete("retired")
            });
            let b = scope.spawn(|| {
                start.wait();
                fs.delete("retired")
            });
            a.join().unwrap().unwrap();
            b.join().unwrap().unwrap();
        });
        fs.delete("retired").unwrap();
    }
    fs.open_write("not-a-directory").unwrap().close().unwrap();
    assert!(fs.delete("not-a-directory/child").is_err());
}

fn cleanup_test_root() {
    let _ = std::fs::remove_dir_all("/tmp/posix_fs_test");
}

#[test]
#[serial_test::serial(file)]
#[cfg(unix)]
fn test_posix_fs_basic() {
    cleanup_test_root();
    let fs = PosixFileSystem::init(&Url::parse(TEST_ROOT).unwrap(), None, None, None).unwrap();
    assert!(!fs.exists("example").unwrap());
    fs.create_dir("example").unwrap();
    assert!(fs.exists("example").unwrap());
    fs.rename("example", "renamed").unwrap();
    assert!(!fs.exists("example").unwrap());
    assert!(fs.exists("renamed").unwrap());
    fs.delete("renamed").unwrap();
    assert!(!fs.exists("renamed").unwrap());
    cleanup_test_root();
}

#[test]
#[serial_test::serial(file)]
#[cfg(unix)]
fn test_posix_fs_read_write_and_mtime() {
    cleanup_test_root();
    let fs = PosixFileSystem::init(&Url::parse(TEST_ROOT).unwrap(), None, None, None).unwrap();
    assert_eq!(fs.last_modified("example").unwrap(), None);
    let data = b"Hello, Cobble!";
    {
        let mut writer = fs.open_write("example").unwrap();
        let written = writer.write(data).unwrap();
        assert_eq!(written, data.len());
        writer.close().unwrap();
    }
    assert_eq!(fs.file_size("example").unwrap(), Some(data.len() as u64));
    assert!(fs.last_modified("example").unwrap().is_some());
    {
        let reader = fs.open_read("example").unwrap();
        let read = reader.read_at(0, data.len()).unwrap();
        assert_eq!(&read[..], data);
    }
    fs.delete("example").unwrap();
    cleanup_test_root();
}

#[test]
#[serial_test::serial(file)]
#[cfg(unix)]
fn test_posix_fs_list() {
    cleanup_test_root();
    let fs = PosixFileSystem::init(&Url::parse(TEST_ROOT).unwrap(), None, None, None).unwrap();
    fs.create_dir("list/subdir").unwrap();
    assert_eq!(fs.file_size("list/subdir").unwrap(), None);
    {
        let mut writer = fs.open_write("list/a.txt").unwrap();
        writer.write(b"a").unwrap();
        writer.close().unwrap();
    }
    {
        let mut writer = fs.open_write("list/b.txt").unwrap();
        writer.write(b"b").unwrap();
        writer.close().unwrap();
    }

    let mut listed = fs.list("list").unwrap();
    listed.sort();
    assert_eq!(
        listed,
        vec![
            "a.txt".to_string(),
            "b.txt".to_string(),
            "subdir".to_string()
        ]
    );
    assert!(fs.list("missing").unwrap().is_empty());
    cleanup_test_root();
}

#[test]
#[serial_test::serial(file)]
#[cfg(unix)]
fn test_posix_fast_copy_uses_hard_link() {
    cleanup_test_root();
    let fs = PosixFileSystem::init(&Url::parse(TEST_ROOT).unwrap(), None, None, None).unwrap();
    fs.create_dir("source").unwrap();
    fs.create_dir("destination").unwrap();
    let mut writer = fs.open_write("source/data.sst").unwrap();
    writer.write(b"fast-copy").unwrap();
    writer.close().unwrap();

    let destination = FastCopyDestination::new(&fs, "destination/data.sst");
    assert!(fs.can_fast_copy_to("source/data.sst", &destination));
    fs.fast_copy_to("source/data.sst", &destination).unwrap();

    let source_metadata = std::fs::metadata("/tmp/posix_fs_test/source/data.sst").unwrap();
    let destination_metadata =
        std::fs::metadata("/tmp/posix_fs_test/destination/data.sst").unwrap();
    assert_eq!(source_metadata.dev(), destination_metadata.dev());
    assert_eq!(source_metadata.ino(), destination_metadata.ino());
    assert_eq!(
        std::fs::read("/tmp/posix_fs_test/destination/data.sst").unwrap(),
        b"fast-copy"
    );
    cleanup_test_root();
}
