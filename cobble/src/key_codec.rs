use crate::error::{Error, Result};
use crate::sst::row_codec::encode_key_ref_into;
use crate::r#type::RefKey;
use bytes::{Bytes, BytesMut};

#[inline]
pub(crate) fn encode_key(bucket: u16, column_family_id: u8, key: &[u8]) -> Bytes {
    let mut encoded = BytesMut::with_capacity(3 + key.len());
    encode_key_ref_into(
        &RefKey::new_with_column_family(bucket, column_family_id, key),
        &mut encoded,
    );
    encoded.freeze()
}

#[inline]
pub(crate) fn encode_scan_key(bucket: u16, column_family_id: u8, key: &[u8]) -> Bytes {
    encode_key(bucket, column_family_id, key)
}

#[inline]
pub(crate) fn encode_scan_key_after(bucket: u16, column_family_id: u8, key: &[u8]) -> Bytes {
    let mut encoded = BytesMut::with_capacity(4 + key.len());
    encode_key_ref_into(
        &RefKey::new_with_column_family(bucket, column_family_id, key),
        &mut encoded,
    );
    encoded.extend_from_slice(&[0]);
    encoded.freeze()
}

pub(crate) fn encode_next_column_family_scan_key(
    bucket: u16,
    column_family_id: u8,
) -> Result<Bytes> {
    let next_column_family_id = column_family_id.checked_add(1).ok_or_else(|| {
        Error::InvalidState(format!(
            "column family id {} cannot derive implicit exclusive scan end",
            column_family_id
        ))
    })?;
    Ok(encode_scan_key(bucket, next_column_family_id, &[]))
}

#[cfg(test)]
mod tests {
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
}
