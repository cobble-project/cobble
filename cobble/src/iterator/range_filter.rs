use crate::error::Result;
use crate::iterator::KvIterator;
use crate::r#type::KvValue;
use bytes::Bytes;

/// Filters encoded scan bounds before values are collected or decoded by deduplication.
pub(crate) struct RangeFilterIterator<I>
where
    I: for<'a> KvIterator<'a>,
{
    inner: I,
    lower_bound_exclusive: Option<Bytes>,
    end_bound: Option<(Bytes, bool)>,
    end_reached: bool,
}

impl<I> RangeFilterIterator<I>
where
    I: for<'a> KvIterator<'a>,
{
    pub(crate) fn new(
        inner: I,
        lower_bound_exclusive: Option<Bytes>,
        end_bound: Option<(Bytes, bool)>,
    ) -> Self {
        Self {
            inner,
            lower_bound_exclusive,
            end_bound,
            end_reached: false,
        }
    }

    fn advance_to_visible(&mut self) -> Result<()> {
        while self.inner.valid() {
            let Some(key) = self.inner.key()? else {
                break;
            };
            if self.end_bound.as_ref().is_some_and(|(end, inclusive)| {
                if *inclusive {
                    key > end.as_ref()
                } else {
                    key >= end.as_ref()
                }
            }) {
                // Unlike a physical block pause, reaching the upper bound is permanent until seek.
                self.end_reached = true;
                break;
            }
            if self
                .lower_bound_exclusive
                .as_deref()
                .is_none_or(|lower_bound| key > lower_bound)
            {
                break;
            }
            if !self.inner.next()? {
                break;
            }
        }
        Ok(())
    }
}

impl<'a, I> KvIterator<'a> for RangeFilterIterator<I>
where
    I: for<'b> KvIterator<'b>,
{
    fn seek(&mut self, target: &[u8]) -> Result<()> {
        self.end_reached = false;
        self.inner.seek(target)?;
        self.advance_to_visible()
    }

    fn seek_to_first(&mut self) -> Result<()> {
        self.end_reached = false;
        self.inner.seek_to_first()?;
        self.advance_to_visible()
    }

    fn next(&mut self) -> Result<bool> {
        if self.end_reached || !self.inner.next()? {
            return Ok(false);
        }
        self.advance_to_visible()?;
        Ok(self.valid())
    }

    fn valid(&self) -> bool {
        !self.end_reached && self.inner.valid()
    }

    fn key(&self) -> Result<Option<&[u8]>> {
        if !self.end_reached {
            self.inner.key()
        } else {
            Ok(None)
        }
    }

    fn take_key(&mut self) -> Result<Option<Bytes>> {
        if !self.end_reached {
            self.inner.take_key()
        } else {
            Ok(None)
        }
    }

    fn take_value(&mut self) -> Result<Option<KvValue>> {
        if !self.end_reached {
            self.inner.take_value()
        } else {
            Ok(None)
        }
    }

    fn set_stop_at_block_boundary(&mut self, enabled: bool) {
        self.inner.set_stop_at_block_boundary(enabled);
    }

    fn clear_stop_at_block_boundary(&mut self) {
        self.inner.clear_stop_at_block_boundary();
    }

    fn stopped_at_block_boundary(&self) -> bool {
        !self.end_reached && self.inner.stopped_at_block_boundary()
    }

    fn current_schema_id(&self) -> Option<u64> {
        if self.end_reached {
            None
        } else {
            self.inner.current_schema_id()
        }
    }
}

#[cfg(test)]
#[path = "../../tests/unit/iterator/range_filter.rs"]
mod tests;
