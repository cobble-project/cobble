use bytes::Bytes;

pub(crate) struct Key {
    /// Logical namespace / group identifier.
    /// Used to partition the keyspace (e.g., different logical groups or column families).
    bucket: u16,

    /// Raw key bytes.
    /// The caller decides the encoding (prefixes, big-endian integers, varints, etc.).
    data: Vec<u8>,
}

#[derive(Clone, Copy)]
pub(crate) struct RefKey<'a> {
    /// Logical namespace / group identifier.
    bucket: u16,
    /// Raw key bytes.
    data: &'a [u8],
}

#[derive(Clone, Copy)]
pub(crate) enum ValueType {
    /// Upsert semantics: insert or overwrite an existing value.
    Put,

    /// Tombstone semantics: marks a key/field as deleted.
    Delete,

    /// Merge semantics: requires a merge operator during reads/compaction.
    Merge,
}

#[derive(Clone)]
pub(crate) struct Column {
    /// Write semantics of this column (Put/Delete/Merge).
    pub(crate) value_type: ValueType,

    /// Raw column bytes.
    data: Vec<u8>,
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
    pub(crate) fn new(bucket: u16, data: Vec<u8>) -> Self {
        Self { bucket, data }
    }

    /// Returns the group identifier.
    pub(crate) fn bucket(&self) -> u16 {
        self.bucket
    }

    /// Returns the raw key bytes.
    pub(crate) fn data(&self) -> &[u8] {
        &self.data
    }
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
    pub(crate) fn new(value_type: ValueType, data: Vec<u8>) -> Self {
        Self { value_type, data }
    }

    /// Returns the value type.
    pub(crate) fn value_type(&self) -> &ValueType {
        &self.value_type
    }

    /// Returns the raw column bytes.
    pub(crate) fn data(&self) -> &[u8] {
        &self.data
    }

    /// Merges this column with a newer column, consuming both.
    ///
    /// Merge semantics:
    /// - If `newer` is `Put` or `Delete`, it replaces `self` entirely (moves `newer`).
    /// - If `newer` is `Merge`, the data is concatenated to `self`'s data (reuses `self`'s buffer).
    ///
    /// This API takes ownership to minimize clones for performance.
    pub(crate) fn merge(mut self, newer: Column) -> Column {
        match newer.value_type {
            ValueType::Put | ValueType::Delete => newer,
            ValueType::Merge => {
                self.data.extend_from_slice(&newer.data);
                self
            }
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
        Bytes::from(value.data)
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
        self.columns.iter().all(|col| {
            matches!(
                col.as_ref().map(|c| c.value_type()),
                Some(ValueType::Put | ValueType::Delete)
            )
        })
    }

    pub(crate) fn terminal_mask(&self) -> Vec<u8> {
        let mask_size = self.columns.len().div_ceil(8).max(1);
        let mut mask = vec![0u8; mask_size];
        for (idx, col) in self.columns.iter().enumerate() {
            if matches!(
                col.as_ref().map(|c| c.value_type()),
                Some(ValueType::Put | ValueType::Delete)
            ) {
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
    pub(crate) fn merge(self, newer: Value) -> Value {
        let max_cols = self.columns.len().max(newer.columns.len());
        let mut merged_columns = Vec::with_capacity(max_cols);

        let mut older_iter = self.columns.into_iter();
        let mut newer_iter = newer.columns.into_iter();

        for _ in 0..max_cols {
            let older_col = older_iter.next().flatten();
            let newer_col = newer_iter.next().flatten();

            let merged = match (older_col, newer_col) {
                (Some(old), Some(new)) => Some(old.merge(new)),
                (Some(old), None) => Some(old),
                (None, Some(new)) => Some(new),
                (None, None) => None,
            };
            merged_columns.push(merged);
        }

        // Prefer expiration from the newer value
        let merged_expired_at = newer.expired_at;

        Value::new_with_expired_at(merged_columns, merged_expired_at)
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

    #[test]
    fn test_column_merge_with_put_replaces() {
        let old = Column::new(ValueType::Put, b"old_data".to_vec());
        let new = Column::new(ValueType::Put, b"new_data".to_vec());

        let merged = old.merge(new);
        assert!(matches!(merged.value_type(), ValueType::Put));
        assert_eq!(merged.data(), b"new_data");
    }

    #[test]
    fn test_column_merge_with_delete_replaces() {
        let old = Column::new(ValueType::Put, b"old_data".to_vec());
        let new = Column::new(ValueType::Delete, b"".to_vec());

        let merged = old.merge(new);
        assert!(matches!(merged.value_type(), ValueType::Delete));
        assert_eq!(merged.data(), b"");
    }

    #[test]
    fn test_column_merge_with_merge_concatenates() {
        let old = Column::new(ValueType::Put, b"hello".to_vec());
        let new = Column::new(ValueType::Merge, b"world".to_vec());

        let merged = old.merge(new);
        // Merge keeps the original value_type and concatenates data
        assert!(matches!(merged.value_type(), ValueType::Put));
        assert_eq!(merged.data(), b"helloworld");
    }

    #[test]
    fn test_column_merge_multiple_merges() {
        let old = Column::new(ValueType::Put, b"a".to_vec());
        let merge1 = Column::new(ValueType::Merge, b"b".to_vec());
        let merge2 = Column::new(ValueType::Merge, b"c".to_vec());

        let merged = old.merge(merge1).merge(merge2);
        assert!(matches!(merged.value_type(), ValueType::Put));
        assert_eq!(merged.data(), b"abc");
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

        let merged = old.merge(new);
        let cols = merged.columns();

        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].as_ref().unwrap().data(), b"new1");
        assert_eq!(cols[1].as_ref().unwrap().data(), b"old2_append");
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

        let merged = old.merge(new);
        let cols = merged.columns();

        assert_eq!(cols.len(), 2);
        // First column unchanged
        assert_eq!(cols[0].as_ref().unwrap().data(), b"old1");
        // Second column replaced
        assert_eq!(cols[1].as_ref().unwrap().data(), b"new2");
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

        let merged = old.merge(new);
        let cols = merged.columns();

        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].as_ref().unwrap().data(), b"old1");
        assert_eq!(cols[1].as_ref().unwrap().data(), b"new2");
    }

    #[test]
    fn test_value_merge_different_lengths() {
        let old = Value::new(vec![Some(Column::new(ValueType::Put, b"old1".to_vec()))]);
        let new = Value::new(vec![
            None,
            Some(Column::new(ValueType::Put, b"new2".to_vec())),
            Some(Column::new(ValueType::Put, b"new3".to_vec())),
        ]);

        let merged = old.merge(new);
        let cols = merged.columns();

        assert_eq!(cols.len(), 3);
        assert_eq!(cols[0].as_ref().unwrap().data(), b"old1");
        assert_eq!(cols[1].as_ref().unwrap().data(), b"new2");
        assert_eq!(cols[2].as_ref().unwrap().data(), b"new3");
    }

    #[test]
    fn test_value_merge_all_none() {
        let old = Value::new(vec![None, None]);
        let new = Value::new(vec![None, None]);

        let merged = old.merge(new);
        let cols = merged.columns();

        assert_eq!(cols.len(), 2);
        assert!(cols[0].is_none());
        assert!(cols[1].is_none());
    }
}
