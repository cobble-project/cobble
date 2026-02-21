//! An iterator over memtable entries, sorted by key and ordinal.
//! This is used for compaction and flushing to ensure that entries are processed in the correct order.
use crate::error::Result;
use crate::iterator::KvIterator;
use bytes::Bytes;
use std::cmp::Ordering;

#[derive(PartialEq, Eq)]
struct OrderedKvEntry<'a> {
    key: &'a [u8],
    value: &'a [u8],
    ordinal: usize,
}

impl Ord for OrderedKvEntry<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        let ord = self.key.cmp(other.key);
        if ord == Ordering::Equal {
            other.ordinal.cmp(&self.ordinal)
        } else {
            ord
        }
    }
}

impl PartialOrd for OrderedKvEntry<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub(crate) struct OrderedMemtableKvIterator<'a> {
    entries: Vec<OrderedKvEntry<'a>>,
    idx: usize,
    current_key: Option<&'a [u8]>,
    current_value: Option<&'a [u8]>,
}

impl<'a> OrderedMemtableKvIterator<'a> {
    pub(crate) fn new(entries: Vec<(&'a [u8], &'a [u8], usize)>) -> Self {
        let mut entries: Vec<OrderedKvEntry<'a>> = entries
            .into_iter()
            .map(|(key, value, ordinal)| OrderedKvEntry {
                key,
                value,
                ordinal,
            })
            .collect();
        entries.sort();
        Self {
            entries,
            idx: 0,
            current_key: None,
            current_value: None,
        }
    }
}

impl<'a> KvIterator<'a> for OrderedMemtableKvIterator<'a> {
    fn seek(&mut self, target: &[u8]) -> Result<()> {
        match self.entries.binary_search_by(|entry| entry.key.cmp(target)) {
            Ok(idx) => self.idx = idx,
            Err(idx) => self.idx = idx,
        }
        self.current_key = None;
        self.current_value = None;
        Ok(())
    }

    fn seek_to_first(&mut self) -> Result<()> {
        self.idx = 0;
        self.current_key = None;
        self.current_value = None;
        Ok(())
    }

    fn next(&mut self) -> Result<bool> {
        if self.idx >= self.entries.len() {
            self.current_key = None;
            self.current_value = None;
            return Ok(false);
        }
        let entry = &self.entries[self.idx];
        self.idx += 1;
        self.current_key = Some(entry.key);
        self.current_value = Some(entry.value);
        Ok(true)
    }

    fn valid(&self) -> bool {
        self.current_key.is_some() && self.current_value.is_some()
    }

    fn key(&self) -> Result<Option<Bytes>> {
        Ok(self.current_key.map(Bytes::copy_from_slice))
    }

    fn key_slice(&self) -> Result<Option<&[u8]>> {
        Ok(self.current_key)
    }

    fn value(&self) -> Result<Option<Bytes>> {
        Ok(self.current_value.map(Bytes::copy_from_slice))
    }

    fn value_slice(&self) -> Result<Option<&[u8]>> {
        Ok(self.current_value)
    }
}
