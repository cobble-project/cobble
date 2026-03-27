use crate::error::Result;
use crate::iterator::KvIterator;
use crate::sst::row_codec::decode_value_masked;
use crate::r#type::{KvValue, Value};
use bytes::Bytes;

pub(crate) struct ColumnMaskingIterator<I> {
    inner: I,
    num_columns: usize,
    selected_columns: Vec<usize>,
    decode_mask: Vec<u8>,
}

impl<I> ColumnMaskingIterator<I> {
    pub(crate) fn new(inner: I, num_columns: usize, selected_columns: &[usize]) -> Self {
        let mask_size = num_columns.div_ceil(8).max(1);
        let last_bits = (num_columns - 1) % 8 + 1;
        let last_mask = (1u8 << last_bits) - 1;
        let mut decode_mask = vec![0u8; mask_size];
        for &column_idx in selected_columns {
            if column_idx < num_columns {
                decode_mask[column_idx / 8] |= 1 << (column_idx % 8);
            }
        }
        decode_mask[mask_size - 1] &= last_mask;
        Self {
            inner,
            num_columns,
            selected_columns: selected_columns.to_vec(),
            decode_mask,
        }
    }

    fn mask_decoded(&self, value: Value) -> Value {
        value.select_columns(self.selected_columns.as_slice())
    }

    fn mask_value(&self, kv_value: KvValue) -> Result<KvValue> {
        match kv_value {
            KvValue::Encoded(mut value_bytes) => {
                let value = decode_value_masked(
                    &mut value_bytes,
                    self.num_columns,
                    self.decode_mask.as_slice(),
                    None,
                )?;
                Ok(KvValue::Decoded(
                    value.select_columns(self.selected_columns.as_slice()),
                ))
            }
            KvValue::Decoded(value) => Ok(KvValue::Decoded(self.mask_decoded(value))),
        }
    }
}

impl<'a, I> KvIterator<'a> for ColumnMaskingIterator<I>
where
    I: KvIterator<'a>,
{
    fn seek(&mut self, target: &[u8]) -> Result<()> {
        self.inner.seek(target)
    }

    fn seek_to_first(&mut self) -> Result<()> {
        self.inner.seek_to_first()
    }

    fn next(&mut self) -> Result<bool> {
        self.inner.next()
    }

    fn valid(&self) -> bool {
        self.inner.valid()
    }

    fn key(&self) -> Result<Option<&[u8]>> {
        self.inner.key()
    }

    fn take_key(&mut self) -> Result<Option<Bytes>> {
        self.inner.take_key()
    }

    fn take_value(&mut self) -> Result<Option<KvValue>> {
        let Some(value) = self.inner.take_value()? else {
            return Ok(None);
        };
        Ok(Some(self.mask_value(value)?))
    }
}
