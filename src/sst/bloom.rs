use crate::error::{Error, Result};
use crate::file::SequentialWriteFile;
use bytes::{Buf, BufMut, Bytes, BytesMut};

#[derive(Debug, Clone)]
pub struct BloomFilter {
    num_bits: u64,
    num_hashes: u32,
    data: Bytes,
}

impl BloomFilter {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(12 + self.data.len());
        buf.put_u32_le(self.num_hashes);
        buf.put_u64_le(self.num_bits);
        buf.put_slice(&self.data);
        buf.freeze()
    }

    pub fn decode(data: Bytes) -> Result<Self> {
        if data.len() < 12 {
            return Err(Error::IoError("Bloom filter block too small".to_string()));
        }
        let mut buf = data.clone();
        let num_hashes = buf.get_u32_le();
        let num_bits = buf.get_u64_le();
        let expected_len = 12 + num_bits.div_ceil(8) as usize;
        if data.len() != expected_len {
            return Err(Error::IoError(format!(
                "Bloom filter size mismatch: expected {}, got {}",
                expected_len,
                data.len()
            )));
        }
        let bitset = data.slice(12..);
        Ok(Self {
            num_bits,
            num_hashes,
            data: bitset,
        })
    }

    pub fn may_contain(&self, key: &[u8]) -> bool {
        if self.num_bits == 0 || self.num_hashes == 0 {
            return false;
        }
        let hash = hash_key(key);
        let (h1, h2) = expand_hash(hash);
        for i in 0..self.num_hashes {
            let bit = h1.wrapping_add((i as u64).wrapping_mul(h2)) % self.num_bits;
            if !test_bit(&self.data, bit) {
                return false;
            }
        }
        true
    }

    pub fn is_empty(&self) -> bool {
        self.num_bits == 0 || self.data.is_empty()
    }

    pub fn size_in_bytes(&self) -> usize {
        // header is u64 + u32
        12 + self.data.len()
    }
}

pub struct BloomFilterBuilder {
    bits_per_key: u32,
    hashes: Vec<u64>,
}

impl BloomFilterBuilder {
    pub fn new(bits_per_key: u32) -> Self {
        Self {
            bits_per_key: bits_per_key.max(1),
            hashes: Vec::new(),
        }
    }

    pub fn add(&mut self, key: &[u8]) {
        self.hashes.push(hash_key(key));
    }

    pub fn finish(self) -> BloomFilter {
        if self.hashes.is_empty() {
            return BloomFilter {
                num_bits: 0,
                num_hashes: 0,
                data: Bytes::new(),
            };
        }
        let mut num_bits = (self.hashes.len() as u64).saturating_mul(self.bits_per_key as u64);
        if num_bits < 64 {
            num_bits = 64;
        }
        let num_hashes = optimal_num_hashes(self.bits_per_key);
        let data_len = num_bits.div_ceil(8) as usize;
        let mut data = vec![0u8; data_len];
        for hash in self.hashes {
            let (h1, h2) = expand_hash(hash);
            for i in 0..num_hashes {
                let bit = h1.wrapping_add((i as u64).wrapping_mul(h2)) % num_bits;
                set_bit(&mut data, bit);
            }
        }
        BloomFilter {
            num_bits,
            num_hashes,
            data: Bytes::from(data),
        }
    }

    pub fn write_to<W: SequentialWriteFile>(self, writer: &mut W) -> Result<usize> {
        if self.hashes.is_empty() {
            return Ok(0);
        }
        let mut num_bits = (self.hashes.len() as u64).saturating_mul(self.bits_per_key as u64);
        if num_bits < 64 {
            num_bits = 64;
        }
        let num_hashes = optimal_num_hashes(self.bits_per_key);
        let data_len = num_bits.div_ceil(8) as usize;
        let mut data = vec![0u8; data_len];
        for hash in self.hashes {
            let (h1, h2) = expand_hash(hash);
            for i in 0..num_hashes {
                let bit = h1.wrapping_add((i as u64).wrapping_mul(h2)) % num_bits;
                set_bit(&mut data, bit);
            }
        }
        let mut buf = [0u8; 12];
        buf[..4].copy_from_slice(&num_hashes.to_le_bytes());
        buf[4..12].copy_from_slice(&num_bits.to_le_bytes());
        writer.write(&buf)?;
        writer.write(&data)?;
        Ok(12 + data_len)
    }
}

fn optimal_num_hashes(bits_per_key: u32) -> u32 {
    let k = (bits_per_key as f64 * 0.69).round() as u32;
    k.max(1)
}

fn hash_key(key: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325;
    for &b in key {
        hash ^= b as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn expand_hash(hash: u64) -> (u64, u64) {
    let h1 = hash;
    let mut h2 = hash.rotate_right(17);
    if h2 == 0 {
        h2 = 0x9e3779b97f4a7c15;
    }
    (h1, h2)
}

fn set_bit(data: &mut [u8], bit: u64) {
    let idx = (bit / 8) as usize;
    let mask = 1u8 << (bit % 8);
    data[idx] |= mask;
}

fn test_bit(data: &[u8], bit: u64) -> bool {
    let idx = (bit / 8) as usize;
    let mask = 1u8 << (bit % 8);
    data.get(idx).is_some_and(|b| b & mask != 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bloom_filter_roundtrip() {
        let mut builder = BloomFilterBuilder::new(10);
        builder.add(b"alpha");
        builder.add(b"beta");
        builder.add(b"gamma");
        let filter = builder.finish();
        let encoded = filter.encode();
        let decoded = BloomFilter::decode(encoded).unwrap();
        assert!(decoded.may_contain(b"alpha"));
        assert!(decoded.may_contain(b"beta"));
        assert!(decoded.may_contain(b"gamma"));
    }
}
