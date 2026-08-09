use super::*;

#[test]
fn column_family_scan_keys_advance_and_reject_overflow() {
    assert_eq!(
        encode_next_column_family_scan_key(7, 3).unwrap(),
        encode_scan_key(7, 4, b"")
    );
    assert!(matches!(
        encode_next_column_family_scan_key(7, u8::MAX),
        Err(Error::InvalidState(_))
    ));

    let base = encode_scan_key(7, 3, b"abc");
    let after = encode_scan_key_after(7, 3, b"abc");
    assert!(after.as_ref() > base.as_ref());
}
