//! Module for memtable implementations and management.
//! Static dispatch is used for method calls to avoid virtual call overhead.
mod hash;
mod iter;
mod manager;
mod vec;
mod vlog;

use crate::error::Result;
use crate::iterator::KvIterator;
use crate::r#type::{RefKey, RefValue};
pub(crate) use hash::HashMemtable;
use std::sync::Arc;
pub(crate) use vec::VecMemtable;

/// Type alias for memtable reclaimer function.
pub(crate) type MemtableReclaimer = Arc<dyn Fn(u64) + Send + Sync>;

#[allow(unused_imports)]
pub(crate) use manager::{
    ActiveMemtable, ImmutableMemtable, MemtableFlushResult, MemtableManager,
    MemtableManagerMetrics, MemtableManagerOptions,
};

/// Trait for memtable implementations.
pub(crate) trait Memtable {
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()>;
    fn put_ref(&mut self, key: &RefKey<'_>, value: &RefValue<'_>, num_columns: usize)
    -> Result<()>;
    fn put_ref_rewritten(
        &mut self,
        key: &RefKey<'_>,
        plan: &vlog::RewrittenValuePlan<'_>,
        num_columns: usize,
    ) -> Result<()>;
    fn get(&self, key: &[u8]) -> Option<&[u8]>;
    fn get_all(&self, key: &[u8]) -> Self::ValueIter<'_>;
    fn remaining_capacity(&self) -> usize;
    fn is_empty(&self) -> bool;
    fn append_blob(&mut self, data: &[u8]) -> Result<usize>;
    fn read_blob(&self, offset: usize, len: usize) -> Option<&[u8]>;
    fn blob_cursor_checkpoint(&self) -> usize;
    fn rollback_blob_cursor(&mut self, checkpoint: usize);

    fn iter(&self) -> Self::KvIter<'_>;

    type ValueIter<'a>: Iterator<Item = &'a [u8]>
    where
        Self: 'a;

    type KvIter<'a>: KvIterator<'a>
    where
        Self: 'a;
}

pub(crate) enum MemtableImpl {
    Hash(HashMemtable),
    Vec(VecMemtable),
}

pub(crate) enum MemtableValueIter<'a> {
    Hash(hash::MemtableValueIter<'a>),
    Vec(vec::MemtableValueIter<'a>),
}

impl<'a> Iterator for MemtableValueIter<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Hash(iter) => iter.next(),
            Self::Vec(iter) => iter.next(),
        }
    }
}

pub(crate) enum MemtableKvIter<'a> {
    Hash(hash::MemtableKvIterator<'a>),
    Vec(vec::MemtableKvIterator<'a>),
}

impl<'a> KvIterator<'a> for MemtableKvIter<'a> {
    fn seek(&mut self, target: &[u8]) -> Result<()> {
        match self {
            Self::Hash(iter) => iter.seek(target),
            Self::Vec(iter) => iter.seek(target),
        }
    }

    fn seek_to_first(&mut self) -> Result<()> {
        match self {
            Self::Hash(iter) => iter.seek_to_first(),
            Self::Vec(iter) => iter.seek_to_first(),
        }
    }

    fn next(&mut self) -> Result<bool> {
        match self {
            Self::Hash(iter) => iter.next(),
            Self::Vec(iter) => iter.next(),
        }
    }

    fn valid(&self) -> bool {
        match self {
            Self::Hash(iter) => iter.valid(),
            Self::Vec(iter) => iter.valid(),
        }
    }

    fn key(&self) -> Result<Option<bytes::Bytes>> {
        match self {
            Self::Hash(iter) => iter.key(),
            Self::Vec(iter) => iter.key(),
        }
    }

    fn key_slice(&self) -> Result<Option<&[u8]>> {
        match self {
            Self::Hash(iter) => iter.key_slice(),
            Self::Vec(iter) => iter.key_slice(),
        }
    }

    fn value(&self) -> Result<Option<bytes::Bytes>> {
        match self {
            Self::Hash(iter) => iter.value(),
            Self::Vec(iter) => iter.value(),
        }
    }

    fn value_slice(&self) -> Result<Option<&[u8]>> {
        match self {
            Self::Hash(iter) => iter.value_slice(),
            Self::Vec(iter) => iter.value_slice(),
        }
    }
}

impl Memtable for MemtableImpl {
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        match self {
            Self::Hash(memtable) => memtable.put(key, value),
            Self::Vec(memtable) => memtable.put(key, value),
        }
    }

    fn put_ref(
        &mut self,
        key: &RefKey<'_>,
        value: &RefValue<'_>,
        num_columns: usize,
    ) -> Result<()> {
        match self {
            Self::Hash(memtable) => memtable.put_ref(key, value, num_columns),
            Self::Vec(memtable) => memtable.put_ref(key, value, num_columns),
        }
    }

    fn put_ref_rewritten(
        &mut self,
        key: &RefKey<'_>,
        plan: &vlog::RewrittenValuePlan<'_>,
        num_columns: usize,
    ) -> Result<()> {
        match self {
            Self::Hash(memtable) => memtable.put_ref_rewritten(key, plan, num_columns),
            Self::Vec(memtable) => memtable.put_ref_rewritten(key, plan, num_columns),
        }
    }

    fn get(&self, key: &[u8]) -> Option<&[u8]> {
        match self {
            Self::Hash(memtable) => memtable.get(key),
            Self::Vec(memtable) => memtable.get(key),
        }
    }

    fn get_all(&self, key: &[u8]) -> Self::ValueIter<'_> {
        match self {
            Self::Hash(memtable) => MemtableValueIter::Hash(memtable.get_all(key)),
            Self::Vec(memtable) => MemtableValueIter::Vec(memtable.get_all(key)),
        }
    }

    fn remaining_capacity(&self) -> usize {
        match self {
            Self::Hash(memtable) => memtable.remaining_capacity(),
            Self::Vec(memtable) => memtable.remaining_capacity(),
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            Self::Hash(memtable) => memtable.is_empty(),
            Self::Vec(memtable) => memtable.is_empty(),
        }
    }

    fn append_blob(&mut self, data: &[u8]) -> Result<usize> {
        match self {
            Self::Hash(memtable) => memtable.append_blob(data),
            Self::Vec(memtable) => memtable.append_blob(data),
        }
    }

    fn read_blob(&self, offset: usize, len: usize) -> Option<&[u8]> {
        match self {
            Self::Hash(memtable) => memtable.read_blob(offset, len),
            Self::Vec(memtable) => memtable.read_blob(offset, len),
        }
    }

    fn blob_cursor_checkpoint(&self) -> usize {
        match self {
            Self::Hash(memtable) => memtable.blob_cursor_checkpoint(),
            Self::Vec(memtable) => memtable.blob_cursor_checkpoint(),
        }
    }

    fn rollback_blob_cursor(&mut self, checkpoint: usize) {
        match self {
            Self::Hash(memtable) => memtable.rollback_blob_cursor(checkpoint),
            Self::Vec(memtable) => memtable.rollback_blob_cursor(checkpoint),
        }
    }

    fn iter(&self) -> Self::KvIter<'_> {
        match self {
            Self::Hash(memtable) => MemtableKvIter::Hash(memtable.iter()),
            Self::Vec(memtable) => MemtableKvIter::Vec(memtable.iter()),
        }
    }

    type ValueIter<'a>
        = MemtableValueIter<'a>
    where
        Self: 'a;

    type KvIter<'a>
        = MemtableKvIter<'a>
    where
        Self: 'a;
}
