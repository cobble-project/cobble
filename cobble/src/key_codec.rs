use crate::error::{Error, Result};
use crate::sst::row_codec::encode_key_ref_into;
use crate::r#type::{ENCODED_KEY_PREFIX_BYTES, RefKey};
use bytes::{Bytes, BytesMut};

#[inline]
pub(crate) fn encode_key(bucket: u16, column_family_id: u8, key: &[u8]) -> Bytes {
    let mut encoded = BytesMut::with_capacity(ENCODED_KEY_PREFIX_BYTES + key.len());
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
    let mut encoded = BytesMut::with_capacity(ENCODED_KEY_PREFIX_BYTES + key.len() + 1);
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
#[path = "../tests/unit/key_codec.rs"]
mod tests;
