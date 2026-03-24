use crate::error::{Error, Result};
use bytes::{BufMut, Bytes, BytesMut};
use std::convert::TryInto;

const PARQUET_META_MAGIC: &[u8; 4] = b"pqm1";

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ParquetRowGroupRange {
    pub(crate) start_key: Vec<u8>,
    pub(crate) end_key: Vec<u8>,
}

#[derive(Clone, Debug)]
pub(crate) struct ParquetMeta {
    row_count: u64,
    row_groups: Vec<ParquetRowGroupRange>,
}

impl ParquetMeta {
    pub(crate) fn new(row_count: u64, row_groups: Vec<ParquetRowGroupRange>) -> Self {
        Self {
            row_count,
            row_groups,
        }
    }

    pub(crate) fn row_count(&self) -> u64 {
        self.row_count
    }

    pub(crate) fn row_groups(&self) -> &[ParquetRowGroupRange] {
        &self.row_groups
    }

    pub(crate) fn encode(self) -> Bytes {
        let mut out = BytesMut::with_capacity(
            4 + 8
                + 4
                + self
                    .row_groups
                    .iter()
                    .map(|group| 8 + group.start_key.len() + group.end_key.len())
                    .sum::<usize>(),
        );
        out.extend_from_slice(PARQUET_META_MAGIC);
        out.put_u64_le(self.row_count);
        out.put_u32_le(self.row_groups.len() as u32);
        for group in self.row_groups {
            out.put_u32_le(group.start_key.len() as u32);
            out.extend_from_slice(&group.start_key);
            out.put_u32_le(group.end_key.len() as u32);
            out.extend_from_slice(&group.end_key);
        }
        out.freeze()
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 16 {
            return Err(Error::IoError(format!(
                "Invalid parquet meta size: expected at least 16, got {}",
                bytes.len()
            )));
        }
        if &bytes[..4] != PARQUET_META_MAGIC {
            return Err(Error::IoError("Invalid parquet meta magic".to_string()));
        }

        let row_count = u64::from_le_bytes(
            bytes[4..12]
                .try_into()
                .map_err(|_| Error::IoError("Invalid parquet meta row_count".to_string()))?,
        );
        let row_group_count = u32::from_le_bytes(
            bytes[12..16]
                .try_into()
                .map_err(|_| Error::IoError("Invalid parquet meta row_group_count".to_string()))?,
        ) as usize;

        let mut offset = 16usize;
        let mut row_groups = Vec::with_capacity(row_group_count);
        for _ in 0..row_group_count {
            if offset + 4 > bytes.len() {
                return Err(Error::IoError(
                    "Invalid parquet meta: missing start key length".to_string(),
                ));
            }
            let start_len =
                u32::from_le_bytes(bytes[offset..offset + 4].try_into().map_err(|_| {
                    Error::IoError("Invalid parquet meta start key length".to_string())
                })?) as usize;
            offset += 4;
            if offset + start_len > bytes.len() {
                return Err(Error::IoError(
                    "Invalid parquet meta: start key out of range".to_string(),
                ));
            }
            let start_key = bytes[offset..offset + start_len].to_vec();
            offset += start_len;

            if offset + 4 > bytes.len() {
                return Err(Error::IoError(
                    "Invalid parquet meta: missing end key length".to_string(),
                ));
            }
            let end_len =
                u32::from_le_bytes(bytes[offset..offset + 4].try_into().map_err(|_| {
                    Error::IoError("Invalid parquet meta end key length".to_string())
                })?) as usize;
            offset += 4;
            if offset + end_len > bytes.len() {
                return Err(Error::IoError(
                    "Invalid parquet meta: end key out of range".to_string(),
                ));
            }
            let end_key = bytes[offset..offset + end_len].to_vec();
            offset += end_len;

            row_groups.push(ParquetRowGroupRange { start_key, end_key });
        }
        if offset != bytes.len() {
            return Err(Error::IoError(
                "Invalid parquet meta: trailing bytes detected".to_string(),
            ));
        }
        Ok(Self {
            row_count,
            row_groups,
        })
    }
}

pub(crate) fn decode_meta(meta_bytes: Option<Bytes>) -> Result<Option<ParquetMeta>> {
    let Some(meta) = meta_bytes else {
        return Ok(None);
    };
    ParquetMeta::decode(meta.as_ref()).map(Some)
}

pub(crate) fn decode_meta_row_count(meta_bytes: Option<Bytes>) -> Result<Option<u64>> {
    Ok(decode_meta(meta_bytes)?.map(|meta| meta.row_count()))
}

pub(crate) fn decode_meta_row_group_ranges(
    meta_bytes: Option<Bytes>,
) -> Result<Option<Vec<ParquetRowGroupRange>>> {
    Ok(decode_meta(meta_bytes)?.map(|meta| meta.row_groups().to_vec()))
}
