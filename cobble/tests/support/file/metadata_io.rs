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
    let path = std::path::absolute(path.as_ref())
        .map_err(|err| Error::IoError(format!("invalid metadata path: {err}")))?;
    let parent = path
        .parent()
        .ok_or_else(|| Error::IoError("metadata path has no parent".to_string()))?;
    let url = url::Url::from_directory_path(parent)
        .map_err(|_| Error::IoError("invalid metadata parent directory".to_string()))?;
    let fs = crate::file::FileSystemRegistry::new().get_or_register(url.as_str())?;
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| Error::IoError("invalid metadata file name".to_string()))?;
    let reader = crate::file::atomic_replace::open_read(fs.as_ref(), name)?;
    let bytes = reader.read_at(0, reader.size())?;
    Ok(strip_and_verify(&bytes)?.to_vec())
}
