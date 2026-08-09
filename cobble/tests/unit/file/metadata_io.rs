use super::*;

#[test]
fn test_strip_and_verify_round_trip() {
    let payload = br#"{"hello":"world"}"#;
    let checksum = compute_checksum(payload);
    let trailer = trailer_for_checksum(checksum);
    let mut encoded = payload.to_vec();
    encoded.extend_from_slice(&trailer);
    let decoded = strip_and_verify(&encoded).unwrap();
    assert_eq!(decoded, payload);
}

#[test]
fn test_strip_and_verify_rejects_checksum_mismatch() {
    let payload = br#"{"hello":"world"}"#;
    let checksum = compute_checksum(payload);
    let trailer = trailer_for_checksum(checksum);
    let mut encoded = payload.to_vec();
    encoded.extend_from_slice(&trailer);
    encoded[1] ^= 0xff;
    let err = strip_and_verify(&encoded).unwrap_err();
    assert!(matches!(err, Error::ChecksumMismatch(_)));
}
