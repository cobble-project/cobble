use crate::error::{Error, Result};
use crate::file::RandomAccessFile;
use crate::sst::bloom::BloomFilter;
use crate::sst::compression::{decode_block_bytes, verify_block_checksum};
use crate::sst::format::{Block, Footer};
use std::sync::Arc;

/// Decodes an immutable index or filter-index block directly from its SST file.
///
/// Callers decide independently whether the result belongs in the block cache or a DataFile pin.
pub(crate) fn read_metadata_block(
    file: &dyn RandomAccessFile,
    offset: u64,
    size: u64,
    block_id: u32,
) -> Result<Arc<Block>> {
    if size == 0 {
        return Err(Error::IoError(
            "SST metadata block size is zero".to_string(),
        ));
    }
    let mut block = Block::decode(file.read_at(offset as usize, size as usize)?)?;
    block.set_block_id(block_id);
    Ok(Arc::new(block))
}

/// Returns the offset and size encoded by an SST index or filter-index entry.
pub(crate) fn indexed_block_location(
    block: &Block,
    entry_idx: usize,
    kind: &str,
) -> Result<(u64, usize)> {
    if entry_idx >= block.offsets_len() {
        return Err(Error::IoError(format!(
            "{kind} index out of bounds: {entry_idx}"
        )));
    }
    let value = block.value(entry_idx)?;
    if value.len() != 16 {
        return Err(Error::IoError(format!("Invalid {kind} index entry")));
    }
    let offset = u64::from_le_bytes(value[0..8].try_into().unwrap());
    let size = u64::from_le_bytes(value[8..16].try_into().unwrap()) as usize;
    if size == 0 {
        return Err(Error::IoError(format!("{kind} block size is zero")));
    }
    Ok((offset, size))
}

/// Decodes an SST data block after applying the footer's checksum and compression rules.
pub(crate) fn read_data_block(
    file: &dyn RandomAccessFile,
    footer: &Footer,
    offset: u64,
    size: usize,
    block_id: u32,
) -> Result<Arc<Block>> {
    let data = file.read_at(offset as usize, size)?;
    let verified = verify_block_checksum(data, footer.block_checksums, "SST data block")?;
    let decoded = decode_block_bytes(verified)?;
    let mut block = Block::decode(decoded)?;
    block.set_block_id(block_id);
    Ok(Arc::new(block))
}

pub(crate) fn read_bloom_filter(
    file: &dyn RandomAccessFile,
    offset: u64,
    size: usize,
) -> Result<Arc<BloomFilter>> {
    if size == 0 {
        return Err(Error::IoError("SST filter block size is zero".to_string()));
    }
    Ok(Arc::new(BloomFilter::decode(
        file.read_at(offset as usize, size)?,
    )?))
}
