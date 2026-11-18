//! Tracked file lifetime and reader/writer coverage.

use crate::file::file_manager::tests::{cleanup_test_root, create_test_file_manager};
use crate::file::{File, RandomAccessFile, SequentialWriteFile, test_utils};

#[test]
#[serial_test::serial(file)]
fn test_file_manager_create_data_file() {
    let (_fs, fm) = create_test_file_manager();

    // Create a data file
    let (file_id, mut writer) = fm.create_data_file().unwrap();
    assert_eq!(file_id, 1);
    assert!(fm.has_data_file(file_id));

    // Write some data
    writer.write(b"test data").unwrap();
    writer.close().unwrap();

    // Verify we can read it back
    let reader = fm.open_data_file_reader(file_id).unwrap();
    let data = reader.read_at(0, 9).unwrap();
    assert_eq!(&data[..], b"test data");

    cleanup_test_root();
}

#[test]
#[serial_test::serial(file)]
fn test_file_manager_remove_data_file() {
    let (fs, fm) = create_test_file_manager();

    // Create and write a file
    let (file_id, mut writer) = fm.create_data_file().unwrap();
    writer.write(b"data").unwrap();
    writer.close().unwrap();
    drop(writer); // Drop the writer to release the Arc reference

    let path = fm.get_data_file_path(file_id).unwrap();
    assert!(fs.exists(&path).unwrap());

    // Remove with delete
    fm.remove_data_file(file_id).unwrap();
    assert!(!fm.has_data_file(file_id));
    test_utils::wait_for_file_deletion(&fs, &path);
    assert!(!fs.exists(&path).unwrap());

    cleanup_test_root();
}

#[test]
#[serial_test::serial(file)]
fn test_file_manager_remove_data_file_with_snapshot_ref() {
    let (fs, fm) = create_test_file_manager();

    let (file_id, mut writer) = fm.create_data_file().unwrap();
    writer.write(b"data").unwrap();
    writer.close().unwrap();
    drop(writer);

    let tracked = fm.data_file_ref(file_id).unwrap();
    let path = fm.get_data_file_path(file_id).unwrap();

    fm.remove_data_file(file_id).unwrap();
    assert!(!fm.has_data_file(file_id));
    assert!(fs.exists(&path).unwrap());

    tracked.dereference();
    assert!(fs.exists(&path).unwrap());
    drop(tracked);
    test_utils::wait_for_file_deletion(&fs, &path);
    assert!(!fs.exists(&path).unwrap());

    cleanup_test_root();
}
