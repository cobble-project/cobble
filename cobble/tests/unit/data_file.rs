use super::*;

fn make_file() -> DataFile {
    DataFile::new_untracked(
        DataFileType::SSTable,
        b"a".to_vec(),
        b"z".to_vec(),
        1,
        0,
        100,
        0..=u16::MAX,
        0..=u16::MAX,
    )
}

#[test]
fn test_data_file_max_expired_at_defaults_to_zero() {
    let file = make_file();
    assert_eq!(file.max_expired_at(), 0);
}

#[test]
fn test_data_file_is_fully_expired() {
    let file = make_file();
    file.set_max_expired_at(1000);
    assert!(!file.is_fully_expired(500));
    assert!(!file.is_fully_expired(999));
    assert!(file.is_fully_expired(1000));
    assert!(file.is_fully_expired(1001));
}

#[test]
fn test_data_file_zero_max_expired_at_never_expires() {
    let file = make_file();
    assert!(!file.is_fully_expired(0));
    assert!(!file.is_fully_expired(u32::MAX));
}
