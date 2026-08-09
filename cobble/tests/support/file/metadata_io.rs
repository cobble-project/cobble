use super::*;
use std::path::Path;

pub fn encode_metadata_payload_for_test(payload: &[u8]) -> Vec<u8> {
    let checksum = compute_checksum(payload);
    let trailer = trailer_for_checksum(checksum);
    let mut encoded = payload.to_vec();
    encoded.extend_from_slice(&trailer);
    encoded
}

pub fn read_metadata_payload_from_path_for_test(path: impl AsRef<Path>) -> Result<Vec<u8>> {
    let path = path.as_ref();
    let bytes = std::fs::read(path).map_err(|err| {
        Error::IoError(format!(
            "Failed to read metadata file '{}': {}",
            path.display(),
            err
        ))
    })?;
    Ok(strip_and_verify(&bytes)?.to_vec())
}
