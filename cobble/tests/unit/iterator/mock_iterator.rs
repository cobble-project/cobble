use crate::iterator::KvIterator;
use crate::r#type::KvValue;
use bytes::Bytes;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

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

    fn key(&self) -> crate::error::Result<Option<&[u8]>> {
        if self.valid() {
            Ok(Some(self.entries[self.index].0.as_ref()))
        } else {
            Ok(None)
        }
    }

    fn take_key(&mut self) -> crate::error::Result<Option<Bytes>> {
        if self.valid() {
            Ok(Some(self.entries[self.index].0.clone()))
        } else {
            Ok(None)
        }
    }

    fn take_value(&mut self) -> crate::error::Result<Option<KvValue>> {
        if self.valid() {
            Ok(Some(KvValue::encoded(self.entries[self.index].1.clone())))
        } else {
            Ok(None)
        }
    }
}

#[derive(Default)]
pub(crate) struct IteratorCounts {
    pub(crate) next: AtomicUsize,
    pub(crate) value_keys: Mutex<Vec<Bytes>>,
}

/// Records work at the physical-input boundary, optionally pausing before an entry.
pub(crate) struct CountingMockIterator {
    inner: MockIterator,
    counts: Arc<IteratorCounts>,
    pause_after_index: Option<usize>,
    stop_enabled: bool,
    stopped: bool,
    pending_resume: bool,
}

impl CountingMockIterator {
    pub(crate) fn new<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        entries: Vec<(K, V)>,
    ) -> (Self, Arc<IteratorCounts>) {
        let counts = Arc::new(IteratorCounts::default());
        (
            Self {
                inner: MockIterator::new(entries),
                counts: Arc::clone(&counts),
                pause_after_index: None,
                stop_enabled: false,
                stopped: false,
                pending_resume: false,
            },
            counts,
        )
    }

    pub(crate) fn with_pause_after_index(mut self, index: usize) -> Self {
        self.pause_after_index = Some(index);
        self
    }
}

impl<'a> KvIterator<'a> for CountingMockIterator {
    fn seek(&mut self, target: &[u8]) -> crate::error::Result<()> {
        self.stopped = false;
        self.pending_resume = false;
        self.inner.seek(target)
    }

    fn seek_to_first(&mut self) -> crate::error::Result<()> {
        self.stopped = false;
        self.pending_resume = false;
        self.inner.seek_to_first()
    }

    fn next(&mut self) -> crate::error::Result<bool> {
        self.counts.next.fetch_add(1, Ordering::Relaxed);
        if self.stopped {
            return Ok(false);
        }
        if self.pending_resume {
            self.pending_resume = false;
        } else if self.stop_enabled && self.pause_after_index == Some(self.inner.index) {
            self.stopped = true;
            self.pending_resume = true;
            return Ok(false);
        }
        self.inner.next()
    }

    fn valid(&self) -> bool {
        !self.stopped && self.inner.valid()
    }

    fn key(&self) -> crate::error::Result<Option<&[u8]>> {
        if self.valid() {
            self.inner.key()
        } else {
            Ok(None)
        }
    }

    fn take_key(&mut self) -> crate::error::Result<Option<Bytes>> {
        if self.valid() {
            self.inner.take_key()
        } else {
            Ok(None)
        }
    }

    fn take_value(&mut self) -> crate::error::Result<Option<KvValue>> {
        if !self.valid() {
            return Ok(None);
        }
        if let Some(key) = self.inner.key()? {
            self.counts
                .value_keys
                .lock()
                .unwrap()
                .push(Bytes::copy_from_slice(key));
        }
        self.inner.take_value()
    }

    fn set_stop_at_block_boundary(&mut self, enabled: bool) {
        self.stop_enabled = enabled;
    }

    fn clear_stop_at_block_boundary(&mut self) {
        self.stopped = false;
    }

    fn stopped_at_block_boundary(&self) -> bool {
        self.stopped
    }
}
