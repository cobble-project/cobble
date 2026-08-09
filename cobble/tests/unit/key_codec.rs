use super::*;

#[test]
fn next_column_family_scan_key_uses_next_family_prefix() {
    assert_eq!(
        encode_next_column_family_scan_key(7, 3).unwrap(),
        encode_scan_key(7, 4, b"")
    );
}

#[test]
fn next_column_family_scan_key_rejects_overflow() {
    assert!(matches!(
        encode_next_column_family_scan_key(7, u8::MAX),
        Err(Error::InvalidState(_))
    ));
}

#[test]
fn scan_key_after_is_strictly_after_original_key() {
    let base = encode_scan_key(7, 3, b"abc");
    let after = encode_scan_key_after(7, 3, b"abc");
    assert!(after.as_ref() > base.as_ref());
}
