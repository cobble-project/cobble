use crate::iterator::KvIterator;
use bytes::Bytes;

/// A simple mock iterator for testing
pub(crate) struct MockIterator {
    entries: Vec<(Bytes, Bytes)>,
    index: usize,
}

impl MockIterator {
    pub(crate) fn new<K: AsRef<[u8]>, V: AsRef<[u8]>>(entries: Vec<(K, V)>) -> Self {
        Self {
            entries: entries
                .into_iter()
                .map(|(k, v)| {
                    (
                        Bytes::copy_from_slice(k.as_ref()),
                        Bytes::copy_from_slice(v.as_ref()),
                    )
                })
                .collect(),
            index: usize::MAX, // Invalid until seek
        }
    }
}

impl<'a> KvIterator<'a> for MockIterator {
    fn seek(&mut self, target: &[u8]) -> crate::error::Result<()> {
        self.index = self
            .entries
            .iter()
            .position(|(k, _)| k.as_ref() >= target)
            .unwrap_or(self.entries.len());
        Ok(())
    }

    fn seek_to_first(&mut self) -> crate::error::Result<()> {
        self.index = 0;
        Ok(())
    }

    fn next(&mut self) -> crate::error::Result<bool> {
        if self.index < self.entries.len() {
            self.index += 1;
            Ok(self.index < self.entries.len())
        } else {
            Ok(false)
        }
    }

    fn valid(&self) -> bool {
        self.index < self.entries.len()
    }

    fn key(&self) -> crate::error::Result<Option<Bytes>> {
        if self.valid() {
            Ok(Some(self.entries[self.index].0.clone()))
        } else {
            Ok(None)
        }
    }

    fn key_slice(&self) -> crate::error::Result<Option<&[u8]>> {
        if self.valid() {
            Ok(Some(self.entries[self.index].0.as_ref()))
        } else {
            Ok(None)
        }
    }

    fn value(&self) -> crate::error::Result<Option<Bytes>> {
        if self.valid() {
            Ok(Some(self.entries[self.index].1.clone()))
        } else {
            Ok(None)
        }
    }

    fn value_slice(&self) -> crate::error::Result<Option<&[u8]>> {
        if self.valid() {
            Ok(Some(self.entries[self.index].1.as_ref()))
        } else {
            Ok(None)
        }
    }
}
