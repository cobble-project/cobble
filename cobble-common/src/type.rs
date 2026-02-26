use crate::error::{Error, Result};
use crate::merge_operator::{MergeOperator, ValueMergeOperator};
use bytes::{Buf, BufMut, Bytes, BytesMut};

pub(crate) struct Key {
    /// Logical namespace / group identifier.
    /// Used to partition the keyspace (e.g., different logical groups or column families).
    bucket: u16,

    /// Raw key bytes.
    /// The caller decides the encoding (prefixes, big-endian integers, varints, etc.).
    data: Bytes,
}

#[derive(Clone, Copy)]
pub(crate) struct RefKey<'a> {
    /// Logical namespace / group identifier.
    bucket: u16,
    /// Raw key bytes.
    data: &'a [u8],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub(crate) enum ValueType {
    /// Upsert semantics: insert or overwrite an existing value.
    Put = 0b0000_0001,

    /// Tombstone semantics: marks a key/field as deleted.
    Delete = 0b0001_0001,

    /// Merge semantics: requires a merge operator during reads/compaction.
    Merge = 0b0000_0010,

    /// Upsert semantics where payload references value-log storage.
    PutSeparated = 0b0000_0101,

    /// Merge semantics where payload references value-log storage.
    MergeSeparated = 0b0000_0110,

    /// Lazy merge chain for separated payloads during compaction.
    MergeSeparatedArray = 0b0000_1110,

    /// Lazy merge chain for separated payloads during compaction, which is also a terminal value.
    PutSeparatedArray = 0b0000_1111,
}

const VALUE_TYPE_TERMINAL_BIT: u8 = 0b0000_0001;
const VALUE_TYPE_MERGE_BIT: u8 = 0b0000_0010;
const VALUE_TYPE_SEPARATED_BIT: u8 = 0b0000_0100;
const VALUE_TYPE_ARRAY_BIT: u8 = 0b0000_1000;

// The anti-mask for merge-separated array items rejects Delete and Array variants.
const VALUE_TYPE_MERGE_ARRAY_ITEM_ANTI_MASK: u8 = 0b0001_1000;

#[derive(Clone)]
pub(crate) struct Column {
    /// Write semantics of this column (Put/Delete/Merge).
    pub(crate) value_type: ValueType,

    /// Raw column bytes.
    data: Bytes,
}

#[derive(Clone, Copy)]
pub(crate) struct RefColumn<'a> {
    /// Write semantics of this column (Put/Delete/Merge).
    pub(crate) value_type: ValueType,
    /// Raw column bytes.
    data: &'a [u8],
}

pub(crate) struct Value {
    /// A value may consist of multiple logical columns/fields.
    /// Each column is optional and may be absent within a value.
    pub(crate) columns: Vec<Option<Column>>,
    /// Optional expiration timestamp (seconds since epoch).
    pub(crate) expired_at: Option<u32>,
}

pub(crate) struct RefValue<'a> {
    /// A value may consist of multiple logical columns/fields.
    /// Each column is optional and may be absent within a value.
    pub(crate) columns: Vec<Option<RefColumn<'a>>>,
    /// Optional expiration timestamp (seconds since epoch).
    pub(crate) expired_at: Option<u32>,
}

impl Key {
    /// Creates a new `Key`.
    ///
    /// \- `bucket`: logical namespace / group id
    /// \- `data`: raw key bytes
    pub(crate) fn new(bucket: u16, data: impl Into<Bytes>) -> Self {
        Self {
            bucket,
            data: data.into(),
        }
    }

    /// Returns the group identifier.
    pub(crate) fn bucket(&self) -> u16 {
        self.bucket
    }

    /// Returns the raw key bytes.
    pub(crate) fn data(&self) -> &Bytes {
        &self.data
    }
}

impl ValueType {
    #[inline]
    pub(crate) fn encode_tag(self) -> u8 {
        self as u8
    }

    #[inline]
    pub(crate) fn decode_tag(byte: u8) -> Result<Self> {
        match byte {
            x if x == ValueType::Put as u8 => Ok(ValueType::Put),
            x if x == ValueType::Delete as u8 => Ok(ValueType::Delete),
            x if x == ValueType::Merge as u8 => Ok(ValueType::Merge),
            x if x == ValueType::PutSeparated as u8 => Ok(ValueType::PutSeparated),
            x if x == ValueType::MergeSeparated as u8 => Ok(ValueType::MergeSeparated),
            x if x == ValueType::MergeSeparatedArray as u8 => Ok(ValueType::MergeSeparatedArray),
            x if x == ValueType::PutSeparatedArray as u8 => Ok(ValueType::PutSeparatedArray),
            _ => Err(Error::IoError(format!("Invalid ValueType: {}", byte))),
        }
    }

    #[inline]
    pub(crate) fn is_terminal(self) -> bool {
        self.encode_tag() & VALUE_TYPE_TERMINAL_BIT != 0
    }

    #[inline]
    pub(crate) fn is_merge_separated_array_item(self) -> bool {
        self.encode_tag() & VALUE_TYPE_MERGE_ARRAY_ITEM_ANTI_MASK == 0
    }

    #[inline]
    pub(crate) fn uses_separated_storage(self) -> bool {
        self.encode_tag() & VALUE_TYPE_SEPARATED_BIT != 0
    }
}

pub(crate) fn decode_merge_separated_array(data: &[u8]) -> Result<Vec<RefColumn<'_>>> {
    let mut columns = Vec::new();
    let mut buf = data;
    while buf.remaining() > 0 {
        if buf.remaining() < 5 {
            return Err(Error::IoError(format!(
                "MergeSeparatedArray entry header too small: {}",
                buf.remaining()
            )));
        }
        let value_type = ValueType::decode_tag(buf.get_u8())?;
        if !value_type.is_merge_separated_array_item() {
            return Err(Error::IoError(format!(
                "MergeSeparatedArray cannot contain nested type {:?}",
                value_type
            )));
        }
        let data_len = buf.get_u32_le() as usize;
        if buf.remaining() < data_len {
            return Err(Error::IoError(format!(
                "MergeSeparatedArray entry truncated: expected {} bytes, got {}",
                data_len,
                buf.remaining()
            )));
        }
        columns.push(RefColumn::new(value_type, &buf[..data_len]));
        buf = &buf[data_len..];
    }
    Ok(columns)
}

pub(crate) fn encode_merge_separated_array(columns: &[RefColumn<'_>]) -> Result<Vec<u8>> {
    let mut total_size = 0usize;
    for column in columns {
        match column.value_type {
            ValueType::MergeSeparatedArray | ValueType::PutSeparatedArray => {
                total_size = total_size.checked_add(column.data().len()).ok_or_else(|| {
                    Error::IoError("MergeSeparatedArray payload size overflow".to_string())
                })?;
            }
            value_type if value_type.is_merge_separated_array_item() => {
                total_size = total_size
                    .checked_add(1 + 4 + column.data().len())
                    .ok_or_else(|| {
                        Error::IoError("MergeSeparatedArray payload size overflow".to_string())
                    })?;
            }
            value_type => {
                return Err(Error::IoError(format!(
                    "MergeSeparatedArray cannot contain type {:?}",
                    value_type
                )));
            }
        }
    }

    let mut buf = BytesMut::with_capacity(total_size);
    for column in columns {
        match column.value_type {
            ValueType::MergeSeparatedArray | ValueType::PutSeparatedArray => {
                buf.put_slice(column.data())
            }
            value_type if value_type.is_merge_separated_array_item() => {
                buf.put_u8(value_type.encode_tag());
                buf.put_u32_le(column.data().len() as u32);
                buf.put_slice(column.data());
            }
            value_type => {
                return Err(Error::IoError(format!(
                    "MergeSeparatedArray cannot contain type {:?}",
                    value_type
                )));
            }
        }
    }
    Ok(buf.to_vec())
}

impl<'a> RefKey<'a> {
    pub(crate) fn new(bucket: u16, data: &'a [u8]) -> Self {
        Self { bucket, data }
    }

    pub(crate) fn bucket(&self) -> u16 {
        self.bucket
    }

    pub(crate) fn data(&self) -> &[u8] {
        self.data
    }

    pub(crate) fn encoded_len(&self) -> usize {
        2 + self.data.len()
    }
}

impl Column {
    /// Creates a new `Column`.
    ///
    /// \- `value_type`: write semantics (Put/Delete/Merge)
    /// \- `data`: raw column bytes
    pub(crate) fn new(value_type: ValueType, data: impl Into<Bytes>) -> Self {
        Self {
            value_type,
            data: data.into(),
        }
    }

    /// Returns the value type.
    pub(crate) fn value_type(&self) -> &ValueType {
        &self.value_type
    }

    /// Returns the raw column bytes.
    pub(crate) fn data(&self) -> &Bytes {
        &self.data
    }

    fn as_ref_column(&self) -> RefColumn<'_> {
        RefColumn::new(self.value_type, self.data())
    }

    /// Merges this column with a newer column, consuming both.
    ///
    /// Merge semantics:
    /// - If `newer` is `Put` or `Delete`, it replaces `self` entirely (moves `newer`).
    /// - If `newer` is `Merge`, the data is concatenated to `self`'s data (reuses `self`'s buffer).
    ///
    /// This API takes ownership to minimize clones for performance.
    pub(crate) fn merge(
        self,
        mut newer: Column,
        merge_operator: &dyn MergeOperator,
    ) -> Result<Column> {
        match newer.value_type {
            ValueType::Put
            | ValueType::PutSeparated
            | ValueType::Delete
            | ValueType::PutSeparatedArray => Ok(newer),
            ValueType::Merge | ValueType::MergeSeparated | ValueType::MergeSeparatedArray => {
                if self.value_type.uses_separated_storage()
                    || newer.value_type.uses_separated_storage()
                {
                    return Self::merge_as_separated_array(self, newer);
                }
                // only merge/put/delete for self and merge for newer can reach here.
                match self.value_type {
                    ValueType::Delete => {
                        // If the existing column is a tombstone, we treat it as an empty value for merging.
                        newer.value_type = ValueType::Put;
                        Ok(newer)
                    }
                    ValueType::Put | ValueType::Merge => Ok(Column::new(
                        self.value_type,
                        merge_operator.merge(Bytes::from(self), Bytes::from(newer))?,
                    )),
                    _ => unreachable!(),
                }
            }
        }
    }

    fn merge_as_separated_array(self, newer: Column) -> Result<Column> {
        let value_type = if self.value_type.is_terminal() {
            ValueType::PutSeparatedArray
        } else {
            ValueType::MergeSeparatedArray
        };
        let older = self.into_merge_separated_array_input()?;
        let refs = [older.as_ref_column(), newer.as_ref_column()];
        let encoded = encode_merge_separated_array(&refs)?;
        Ok(Column::new(value_type, encoded))
    }

    fn into_merge_separated_array_input(self) -> Result<Column> {
        match self.value_type {
            ValueType::MergeSeparatedArray | ValueType::PutSeparatedArray => Ok(self),
            value_type if value_type.is_merge_separated_array_item() => Ok(self),
            value_type => Err(Error::IoError(format!(
                "Cannot encode {:?} into MergeSeparatedArray",
                value_type
            ))),
        }
    }
}

impl<'a> RefColumn<'a> {
    pub(crate) fn new(value_type: ValueType, data: &'a [u8]) -> Self {
        Self { value_type, data }
    }

    pub(crate) fn value_type(&self) -> &ValueType {
        &self.value_type
    }

    pub(crate) fn data(&self) -> &[u8] {
        self.data
    }
}

impl From<Column> for Bytes {
    fn from(value: Column) -> Self {
        value.data
    }
}

impl Value {
    /// Creates a new `Value` from a list of optional columns.
    pub(crate) fn new(columns: Vec<Option<Column>>) -> Self {
        Self::new_with_expired_at(columns, None)
    }

    pub(crate) fn new_with_expired_at(
        columns: Vec<Option<Column>>,
        expired_at: Option<u32>,
    ) -> Self {
        Self {
            columns,
            expired_at,
        }
    }

    /// Returns the optional columns.
    pub(crate) fn columns(&self) -> &[Option<Column>] {
        &self.columns
    }

    /// Returns the expiration timestamp if present.
    pub(crate) fn expired_at(&self) -> Option<u32> {
        self.expired_at
    }

    /// Checks if all columns are terminal (Put or Delete).
    pub(crate) fn is_terminal(&self) -> bool {
        self.columns
            .iter()
            .all(|col| col.as_ref().is_some_and(|c| c.value_type().is_terminal()))
    }

    pub(crate) fn terminal_mask(&self) -> Vec<u8> {
        let mask_size = self.columns.len().div_ceil(8).max(1);
        let mut mask = vec![0u8; mask_size];
        for (idx, col) in self.columns.iter().enumerate() {
            if col
                .as_ref()
                .is_some_and(|column| column.value_type().is_terminal())
            {
                mask[idx / 8] |= 1 << (idx % 8);
            }
        }
        mask
    }

    pub(crate) fn select_columns(mut self, indices: &[usize]) -> Value {
        let mut selected = Vec::with_capacity(indices.len());
        for &idx in indices {
            let column = self.columns.get_mut(idx).and_then(|col_opt| col_opt.take());
            selected.push(column);
        }
        Value::new_with_expired_at(selected, self.expired_at)
    }

    /// Merges this value with a newer value, consuming both.
    ///
    /// Columns at the same position are merged according to their types:
    /// - If the newer column is `None`, keep the older column (moved, not cloned).
    /// - If the newer column exists, merge it with the older column using `Column::merge`.
    ///
    /// The resulting value has the maximum number of columns from both values.
    /// This API takes ownership to minimize clones for performance.
    pub(crate) fn merge(self, newer: Value, merge_operators: &ValueMergeOperator) -> Result<Value> {
        self.merge_with_callback(newer, merge_operators, &mut |_, _| {})
    }

    pub(crate) fn merge_with_callback<F>(
        self,
        newer: Value,
        merge_operators: &ValueMergeOperator,
        on_merge: &mut F,
    ) -> Result<Value>
    where
        F: FnMut(Option<&Column>, Option<&Column>) + ?Sized,
    {
        let max_cols = self.columns.len().max(newer.columns.len());
        let mut merged_columns = Vec::with_capacity(max_cols);

        let mut older_iter = self.columns.into_iter();
        let mut newer_iter = newer.columns.into_iter();

        for column_idx in 0..max_cols {
            let older_col = older_iter.next().flatten();
            let newer_col = newer_iter.next().flatten();

            let merged = match (older_col, newer_col) {
                (Some(old), Some(new)) => {
                    on_merge(Some(&old), Some(&new));
                    if old.data().is_empty() {
                        Some(new)
                    } else {
                        Some(old.merge(new, merge_operators.operator(column_idx))?)
                    }
                }
                (Some(old), None) => {
                    on_merge(Some(&old), None);
                    Some(old)
                }
                (None, Some(new)) => {
                    on_merge(None, Some(&new));
                    Some(new)
                }
                (None, None) => None,
            };
            merged_columns.push(merged);
        }

        // Prefer expiration from the newer value
        let merged_expired_at = newer.expired_at;

        Ok(Value::new_with_expired_at(
            merged_columns,
            merged_expired_at,
        ))
    }
}

impl<'a> RefValue<'a> {
    pub(crate) fn new(columns: Vec<Option<RefColumn<'a>>>) -> Self {
        Self::new_with_expired_at(columns, None)
    }

    pub(crate) fn new_with_expired_at(
        columns: Vec<Option<RefColumn<'a>>>,
        expired_at: Option<u32>,
    ) -> Self {
        Self {
            columns,
            expired_at,
        }
    }

    pub(crate) fn columns(&self) -> &[Option<RefColumn<'a>>] {
        &self.columns
    }

    pub(crate) fn expired_at(&self) -> Option<u32> {
        self.expired_at
    }

    pub(crate) fn encoded_len(&self, num_columns: usize) -> usize {
        let bmp_size = if num_columns <= 1 {
            0
        } else {
            num_columns.div_ceil(8)
        };
        let present_count = self
            .columns
            .iter()
            .take(num_columns)
            .filter(|c| c.is_some())
            .count();
        let mut size = 4 + bmp_size;
        for col in self.columns.iter().take(num_columns).flatten() {
            size += 1 + 4 + col.data().len();
        }
        if present_count > 0 {
            size -= 4;
        }
        size
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::merge_operator::{MergeOperator, default_merge_operator_ref};
    use std::sync::Arc;

    struct PanicMergeOperator;

    impl MergeOperator for PanicMergeOperator {
        fn merge(&self, _existing_value: Bytes, _value: Bytes) -> Result<Bytes> {
            panic!("merge operator should not be invoked for empty old value");
        }
    }

    #[test]
    fn test_column_merge_with_put_replaces() {
        let old = Column::new(ValueType::Put, b"old_data".to_vec());
        let new = Column::new(ValueType::Put, b"new_data".to_vec());

        let merged = old
            .merge(new, default_merge_operator_ref().as_ref())
            .unwrap();
        assert!(matches!(merged.value_type(), ValueType::Put));
        assert_eq!(merged.data().as_ref(), b"new_data");
    }

    #[test]
    fn test_column_merge_with_delete_replaces() {
        let old = Column::new(ValueType::Put, b"old_data".to_vec());
        let new = Column::new(ValueType::Delete, b"".to_vec());

        let merged = old
            .merge(new, default_merge_operator_ref().as_ref())
            .unwrap();
        assert!(matches!(merged.value_type(), ValueType::Delete));
        assert_eq!(merged.data().as_ref(), b"");
    }

    #[test]
    fn test_column_merge_with_merge_concatenates() {
        let old = Column::new(ValueType::Put, b"hello".to_vec());
        let new = Column::new(ValueType::Merge, b"world".to_vec());

        let merged = old
            .merge(new, default_merge_operator_ref().as_ref())
            .unwrap();
        // Merge keeps the original value_type and concatenates data
        assert!(matches!(merged.value_type(), ValueType::Put));
        assert_eq!(merged.data().as_ref(), b"helloworld");
    }

    #[test]
    fn test_column_merge_multiple_merges() {
        let old = Column::new(ValueType::Put, b"a".to_vec());
        let merge1 = Column::new(ValueType::Merge, b"b".to_vec());
        let merge2 = Column::new(ValueType::Merge, b"c".to_vec());

        let merged = old
            .merge(merge1, default_merge_operator_ref().as_ref())
            .unwrap()
            .merge(merge2, default_merge_operator_ref().as_ref())
            .unwrap();
        assert!(matches!(merged.value_type(), ValueType::Put));
        assert_eq!(merged.data().as_ref(), b"abc");
    }

    #[test]
    fn test_value_merge_all_columns_present() {
        let old = Value::new(vec![
            Some(Column::new(ValueType::Put, b"old1".to_vec())),
            Some(Column::new(ValueType::Put, b"old2".to_vec())),
        ]);
        let new = Value::new(vec![
            Some(Column::new(ValueType::Put, b"new1".to_vec())),
            Some(Column::new(ValueType::Merge, b"_append".to_vec())),
        ]);

        let merged = old.merge(new, &ValueMergeOperator::empty()).unwrap();
        let cols = merged.columns();

        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].as_ref().unwrap().data().as_ref(), b"new1");
        assert_eq!(cols[1].as_ref().unwrap().data().as_ref(), b"old2_append");
    }

    #[test]
    fn test_value_merge_partial_columns() {
        let old = Value::new(vec![
            Some(Column::new(ValueType::Put, b"old1".to_vec())),
            Some(Column::new(ValueType::Put, b"old2".to_vec())),
        ]);
        let new = Value::new(vec![
            None,
            Some(Column::new(ValueType::Put, b"new2".to_vec())),
        ]);

        let merged = old.merge(new, &ValueMergeOperator::empty()).unwrap();
        let cols = merged.columns();

        assert_eq!(cols.len(), 2);
        // First column unchanged
        assert_eq!(cols[0].as_ref().unwrap().data().as_ref(), b"old1");
        // Second column replaced
        assert_eq!(cols[1].as_ref().unwrap().data().as_ref(), b"new2");
    }

    #[test]
    fn test_value_merge_new_column_fills_none() {
        let old = Value::new(vec![
            Some(Column::new(ValueType::Put, b"old1".to_vec())),
            None,
        ]);
        let new = Value::new(vec![
            None,
            Some(Column::new(ValueType::Put, b"new2".to_vec())),
        ]);

        let merged = old.merge(new, &ValueMergeOperator::empty()).unwrap();
        let cols = merged.columns();

        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].as_ref().unwrap().data().as_ref(), b"old1");
        assert_eq!(cols[1].as_ref().unwrap().data().as_ref(), b"new2");
    }

    #[test]
    fn test_value_merge_different_lengths() {
        let old = Value::new(vec![Some(Column::new(ValueType::Put, b"old1".to_vec()))]);
        let new = Value::new(vec![
            None,
            Some(Column::new(ValueType::Put, b"new2".to_vec())),
            Some(Column::new(ValueType::Put, b"new3".to_vec())),
        ]);

        let merged = old.merge(new, &ValueMergeOperator::empty()).unwrap();
        let cols = merged.columns();

        assert_eq!(cols.len(), 3);
        assert_eq!(cols[0].as_ref().unwrap().data().as_ref(), b"old1");
        assert_eq!(cols[1].as_ref().unwrap().data().as_ref(), b"new2");
        assert_eq!(cols[2].as_ref().unwrap().data().as_ref(), b"new3");
    }

    #[test]
    fn test_value_merge_all_none() {
        let old = Value::new(vec![None, None]);
        let new = Value::new(vec![None, None]);

        let merged = old.merge(new, &ValueMergeOperator::empty()).unwrap();
        let cols = merged.columns();

        assert_eq!(cols.len(), 2);
        assert!(cols[0].is_none());
        assert!(cols[1].is_none());
    }

    #[test]
    fn test_encode_decode_merge_separated_array_flatten_nested() {
        let nested_columns = vec![
            Column::new(ValueType::PutSeparated, b"p1".to_vec()),
            Column::new(ValueType::MergeSeparated, b"m1".to_vec()),
        ];
        let nested_refs: Vec<_> = nested_columns.iter().map(Column::as_ref_column).collect();
        let nested = encode_merge_separated_array(&nested_refs).unwrap();
        let encoded_columns = vec![
            Column::new(ValueType::Put, b"inline".to_vec()),
            Column::new(ValueType::MergeSeparatedArray, nested),
            Column::new(ValueType::Merge, b"suffix".to_vec()),
        ];
        let encoded_refs: Vec<_> = encoded_columns.iter().map(Column::as_ref_column).collect();
        let encoded = encode_merge_separated_array(&encoded_refs).unwrap();
        let decoded = decode_merge_separated_array(&encoded).unwrap();
        assert_eq!(decoded.len(), 4);
        assert_eq!(decoded[0].value_type, ValueType::Put);
        assert_eq!(decoded[0].data(), b"inline");
        assert_eq!(decoded[1].value_type, ValueType::PutSeparated);
        assert_eq!(decoded[1].data(), b"p1");
        assert_eq!(decoded[2].value_type, ValueType::MergeSeparated);
        assert_eq!(decoded[2].data(), b"m1");
        assert_eq!(decoded[3].value_type, ValueType::Merge);
        assert_eq!(decoded[3].data(), b"suffix");
    }

    #[test]
    fn test_decode_merge_separated_array_rejects_nested_type() {
        let mut invalid = BytesMut::new();
        invalid.put_u8(ValueType::MergeSeparatedArray.encode_tag());
        invalid.put_u32_le(3);
        invalid.put_slice(b"bad");
        assert!(decode_merge_separated_array(&invalid).is_err());
    }

    #[test]
    fn test_column_merge_with_separated_creates_array() {
        let old = Column::new(ValueType::PutSeparated, b"p0".to_vec());
        let new = Column::new(ValueType::MergeSeparated, b"m1".to_vec());
        let merged = old
            .merge(new, default_merge_operator_ref().as_ref())
            .unwrap();
        assert_eq!(merged.value_type, ValueType::PutSeparatedArray);
        let decoded = decode_merge_separated_array(merged.data()).unwrap();
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].value_type, ValueType::PutSeparated);
        assert_eq!(decoded[0].data(), b"p0");
        assert_eq!(decoded[1].value_type, ValueType::MergeSeparated);
        assert_eq!(decoded[1].data(), b"m1");
    }

    #[test]
    fn test_put_separated_is_terminal() {
        let value = Value::new(vec![Some(Column::new(
            ValueType::PutSeparated,
            b"p".to_vec(),
        ))]);
        assert!(value.is_terminal());
        assert_eq!(value.terminal_mask(), vec![0b0000_0001]);
    }

    #[test]
    fn test_value_type_bit_semantics() {
        assert_eq!(ValueType::Put.encode_tag() & VALUE_TYPE_TERMINAL_BIT, 1);
        assert_eq!(
            ValueType::Merge.encode_tag() & VALUE_TYPE_MERGE_BIT,
            VALUE_TYPE_MERGE_BIT
        );
        assert_eq!(
            ValueType::PutSeparated.encode_tag() & VALUE_TYPE_SEPARATED_BIT,
            VALUE_TYPE_SEPARATED_BIT
        );
        assert_eq!(
            ValueType::MergeSeparatedArray.encode_tag() & VALUE_TYPE_ARRAY_BIT,
            VALUE_TYPE_ARRAY_BIT
        );
    }

    #[test]
    fn test_value_merge_callback_invoked_with_empty_sides() {
        let old = Value::new(vec![
            Some(Column::new(ValueType::Put, b"old0".to_vec())),
            None,
        ]);
        let new = Value::new(vec![
            None,
            Some(Column::new(ValueType::PutSeparated, b"p1".to_vec())),
            None,
        ]);
        let mut seen = Vec::new();
        let _ = old
            .merge_with_callback(
                new,
                &ValueMergeOperator::empty(),
                &mut |old_col, new_col| {
                    seen.push((old_col.map(|c| c.value_type), new_col.map(|c| c.value_type)));
                },
            )
            .unwrap();
        assert_eq!(
            seen,
            vec![
                (Some(ValueType::Put), None),
                (None, Some(ValueType::PutSeparated)),
            ]
        );
    }

    #[test]
    fn test_value_merge_skips_operator_when_old_missing() {
        let old = Value::new(vec![None]);
        let new = Value::new(vec![Some(Column::new(ValueType::Merge, b"m".to_vec()))]);
        let merge_ops = ValueMergeOperator::new(Arc::new(vec![Arc::new(PanicMergeOperator)]));
        let merged = old.merge(new, &merge_ops).unwrap();
        let col = merged.columns()[0].as_ref().unwrap();
        assert_eq!(col.value_type, ValueType::Merge);
        assert_eq!(col.data().as_ref(), b"m");
    }

    #[test]
    fn test_value_merge_skips_operator_when_old_empty() {
        let old = Value::new(vec![Some(Column::new(ValueType::Put, Bytes::new()))]);
        let new = Value::new(vec![Some(Column::new(ValueType::Merge, b"m".to_vec()))]);
        let merge_ops = ValueMergeOperator::new(Arc::new(vec![Arc::new(PanicMergeOperator)]));
        let merged = old.merge(new, &merge_ops).unwrap();
        let col = merged.columns()[0].as_ref().unwrap();
        assert_eq!(col.value_type, ValueType::Merge);
        assert_eq!(col.data().as_ref(), b"m");
    }
}
