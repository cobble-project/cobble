use crate::error::{Error, Result};
use crate::file::files::{File, ReadAllFile, SequentialWriteFile};
use bytes::Bytes;
use crc32fast::Hasher;
use std::path::Path;

const METADATA_CHECKSUM_MAGIC: &[u8; 4] = b"mcs1";
const METADATA_CHECKSUM_TRAILER_SIZE: usize = 8;

fn checksum_error(message: impl Into<String>) -> Error {
    Error::ChecksumMismatch(message.into())
}

fn trailer_for_checksum(checksum: u32) -> [u8; METADATA_CHECKSUM_TRAILER_SIZE] {
    let mut trailer = [0u8; METADATA_CHECKSUM_TRAILER_SIZE];
    trailer[..4].copy_from_slice(&checksum.to_le_bytes());
    trailer[4..].copy_from_slice(METADATA_CHECKSUM_MAGIC);
    trailer
}

fn compute_checksum(bytes: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(bytes);
    hasher.finalize()
}

fn verified_payload_len(bytes: &[u8]) -> Result<usize> {
    if bytes.len() < METADATA_CHECKSUM_TRAILER_SIZE {
        return Err(checksum_error("trailer missing"));
    }
    let payload_len = bytes.len() - METADATA_CHECKSUM_TRAILER_SIZE;
    let expected = u32::from_le_bytes(
        bytes[payload_len..payload_len + 4]
            .try_into()
            .map_err(|_| checksum_error("trailer decode failed"))?,
    );
    if &bytes[payload_len + 4..] != METADATA_CHECKSUM_MAGIC {
        return Err(checksum_error("magic mismatch"));
    }
    let actual = compute_checksum(&bytes[..payload_len]);
    if actual != expected {
        return Err(checksum_error(format!(
            "mismatch: expected {expected:08x}, got {actual:08x}"
        )));
    }
    Ok(payload_len)
}

fn strip_and_verify(bytes: &[u8]) -> Result<&[u8]> {
    let payload_len = verified_payload_len(bytes)?;
    Ok(&bytes[..payload_len])
}

#[doc(hidden)]
pub fn encode_metadata_payload_for_test(payload: &[u8]) -> Vec<u8> {
    let checksum = compute_checksum(payload);
    let trailer = trailer_for_checksum(checksum);
    let mut encoded = payload.to_vec();
    encoded.extend_from_slice(&trailer);
    encoded
}

#[doc(hidden)]
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

pub(crate) struct MetadataReader<R: ReadAllFile> {
    inner: R,
}

impl<R: ReadAllFile> MetadataReader<R> {
    pub(crate) fn new(inner: R) -> Self {
        Self { inner }
    }

    pub(crate) fn read_all(&self) -> Result<Bytes> {
        let bytes = self.inner.read_all()?;
        let payload_len = verified_payload_len(bytes.as_ref())?;
        Ok(bytes.slice(0..payload_len))
    }
}

pub(crate) struct MetadataWriter<W: SequentialWriteFile> {
    inner: Option<W>,
    checksum_hasher: Hasher,
    closed: bool,
}

impl<W: SequentialWriteFile> MetadataWriter<W> {
    pub(crate) fn new(inner: W) -> Self {
        Self {
            inner: Some(inner),
            checksum_hasher: Hasher::new(),
            closed: false,
        }
    }
}

impl<W: SequentialWriteFile> SequentialWriteFile for MetadataWriter<W> {
    fn write(&mut self, data: &[u8]) -> Result<usize> {
        if self.closed {
            return Err(Error::IoError("Metadata writer already closed".to_string()));
        }
        match self.inner.as_mut() {
            Some(inner) => {
                let written = inner.write(data)?;
                self.checksum_hasher.update(&data[..written]);
                Ok(written)
            }
            None => Err(Error::IoError("Metadata writer already closed".to_string())),
        }
    }
}

impl<W: SequentialWriteFile> File for MetadataWriter<W> {
    fn close(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        let Some(inner) = self.inner.as_mut() else {
            return Ok(());
        };
        let checksum = std::mem::replace(&mut self.checksum_hasher, Hasher::new()).finalize();
        let trailer = trailer_for_checksum(checksum);
        inner.write(&trailer)?;
        inner.close()?;
        self.closed = true;
        Ok(())
    }

    fn size(&self) -> usize {
        self.inner.as_ref().map(|inner| inner.size()).unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
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
}
