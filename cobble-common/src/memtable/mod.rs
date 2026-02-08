mod hash;
mod manager;

use crate::error::Result;
use crate::iterator::KvIterator;

#[allow(unused_imports)]
pub(crate) use manager::{
    ActiveMemtable, ImmutableMemtable, MemtableFlushResult, MemtableManager, MemtableManagerOptions,
};

/// Trait for memtable implementations.
pub(crate) trait Memtable {
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()>;
    fn get(&self, key: &[u8]) -> Option<&[u8]>;
    fn get_all(&self, key: &[u8]) -> Self::ValueIter<'_>;
    fn remaining_capacity(&self) -> usize;

    fn iter(&self) -> Self::KvIter<'_>;

    type ValueIter<'a>: Iterator<Item = &'a [u8]>
    where
        Self: 'a;

    type KvIter<'a>: KvIterator<'a>
    where
        Self: 'a;
}
