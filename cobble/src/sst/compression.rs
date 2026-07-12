//! SST block compression utilities.
use crate::error::{Error, Result};
use crate::file::SequentialWriteFile;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

const BLOCK_COMPRESSION_MAGIC: u32 = 0x4b4c4243;
const BLOCK_HEADER_SIZE: usize = 9;
const BLOCK_CHECKSUM_SIZE: usize = 4;

/// Compression algorithms for SST blocks.
/// Currently only LZ4 is supported, but more may be added in the future.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SstCompressionAlgorithm {
    #[default]
    None,
    Lz4,
}

impl SstCompressionAlgorithm {
    fn id(self) -> u8 {
        match self {
            Self::None => 0,
            Self::Lz4 => 1,
        }
    }

    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Lz4 => "lz4",
        }
    }

    fn from_id(byte: u8) -> Result<Self> {
        match byte {
            0 => Ok(Self::None),
            1 => Ok(Self::Lz4),
            _ => Err(Error::IoError(format!(
                "Invalid compression algorithm: {}",
                byte
            ))),
        }
    }
}

pub(crate) fn write_block<W: SequentialWriteFile>(
    writer: &mut W,
    raw: Bytes,
    compression: SstCompressionAlgorithm,
    checksum_enabled: bool,
) -> Result<usize> {
    let raw_len = raw.len();
    match compression {
        SstCompressionAlgorithm::None => {
            write_block_parts(writer, &[raw.as_ref()], checksum_enabled)
        }
        SstCompressionAlgorithm::Lz4 => {
            let compressed = lz4_flex::compress(raw.as_ref());
            let mut header = [0u8; BLOCK_HEADER_SIZE];
            header[..4].copy_from_slice(&BLOCK_COMPRESSION_MAGIC.to_le_bytes());
            header[4] = compression.id();
            header[5..9].copy_from_slice(&(raw_len as u32).to_le_bytes());
            write_block_parts(
                writer,
                &[header.as_slice(), compressed.as_slice()],
                checksum_enabled,
            )
        }
    }
}

fn write_block_parts<W: SequentialWriteFile>(
    writer: &mut W,
    parts: &[&[u8]],
    checksum_enabled: bool,
) -> Result<usize> {
    let mut written = 0usize;
    let mut hasher = checksum_enabled.then(crc32fast::Hasher::new);
    for part in parts {
        written = written.saturating_add(writer.write(part)?);
        if let Some(hasher) = &mut hasher {
            hasher.update(part);
        }
    }
    if let Some(hasher) = hasher {
        written = written.saturating_add(writer.write(&hasher.finalize().to_le_bytes())?);
    }
    Ok(written)
}

pub(crate) fn verify_block_checksum(
    data: Bytes,
    checksum_enabled: bool,
    description: &str,
) -> Result<Bytes> {
    if !checksum_enabled {
        return Ok(data);
    }
    if data.len() < BLOCK_CHECKSUM_SIZE {
        return Err(Error::ChecksumMismatch(format!(
            "{} checksum trailer missing",
            description
        )));
    }
    let payload_len = data.len() - BLOCK_CHECKSUM_SIZE;
    let expected = u32::from_le_bytes(
        data[payload_len..]
            .try_into()
            .expect("checksum trailer length checked"),
    );
    let actual = crc32fast::hash(&data[..payload_len]);
    if actual != expected {
        return Err(Error::ChecksumMismatch(format!(
            "{}: expected {:08x}, got {:08x}",
            description, expected, actual
        )));
    }
    Ok(data.slice(..payload_len))
}

pub(crate) fn decode_block_bytes(data: Bytes) -> Result<Bytes> {
    if data.len() < 4 {
        return Err(Error::IoError("Block data too small".to_string()));
    }
    let magic = u32::from_le_bytes(
        data[..4]
            .try_into()
            .map_err(|_| Error::IoError("Block data too small".to_string()))?,
    );
    if magic != BLOCK_COMPRESSION_MAGIC {
        return Ok(data);
    }
    if data.len() < BLOCK_HEADER_SIZE {
        return Err(Error::IoError(
            "Block compression header too small".to_string(),
        ));
    }
    let compression = SstCompressionAlgorithm::from_id(data[4])?;
    let uncompressed_len = u32::from_le_bytes(
        data[5..9]
            .try_into()
            .map_err(|_| Error::IoError("Block compression header too small".to_string()))?,
    ) as usize;
    let payload = data.slice(BLOCK_HEADER_SIZE..);
    match compression {
        SstCompressionAlgorithm::None => {
            if payload.len() != uncompressed_len {
                return Err(Error::IoError(format!(
                    "Block size mismatch: expected {}, got {}",
                    uncompressed_len,
                    payload.len()
                )));
            }
            Ok(payload)
        }
        SstCompressionAlgorithm::Lz4 => {
            let decompressed = lz4_flex::decompress(payload.as_ref(), uncompressed_len)
                .map_err(|err| Error::IoError(err.to_string()))?;
            Ok(Bytes::from(decompressed))
        }
    }
}
