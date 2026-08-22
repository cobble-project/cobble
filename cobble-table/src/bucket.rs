use crate::{Result, TableError};

/// Stable bucket assignment for encoded bucket-key prefixes.
///
/// This is exactly Java's `Arrays.hashCode(byte[])`: signed bytes contribute to a wrapping i32
/// accumulator, then `floorMod` selects a bucket.
#[derive(Clone, Debug)]
pub struct BucketHash {
    modulus: i32,
    power_of_two_mask: i32,
}

impl BucketHash {
    pub fn new(total_buckets: u32) -> Result<Self> {
        if !(1..=65_536).contains(&total_buckets) {
            return Err(TableError::codec("bucket count must be in [1, 65536]"));
        }

        Ok(Self {
            modulus: total_buckets as i32,
            power_of_two_mask: if total_buckets.is_power_of_two() {
                total_buckets as i32 - 1
            } else {
                -1
            },
        })
    }

    #[inline]
    fn hash(bytes: &[u8]) -> i32 {
        let mut hash = 1_i32;
        for &byte in bytes {
            hash = hash.wrapping_mul(31).wrapping_add(byte as i8 as i32);
        }
        hash
    }

    #[must_use]
    #[inline]
    pub fn bucket(&self, encoded_bucket_key: &[u8]) -> u16 {
        let hash = Self::hash(encoded_bucket_key);
        // floorMod by a power of two is exactly the corresponding low bits, including for
        // negative two's-complement hashes.
        if self.power_of_two_mask >= 0 {
            (hash & self.power_of_two_mask) as u16
        } else {
            hash.rem_euclid(self.modulus) as u16
        }
    }
}
