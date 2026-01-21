pub(crate) struct Key {
    /// Logical namespace / group identifier.
    /// Used to partition the keyspace (e.g., different logical groups or column families).
    group: u16,

    /// Raw key bytes.
    /// The caller decides the encoding (prefixes, big-endian integers, varints, etc.).
    data: Vec<u8>,
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
    value_type: ValueType,

    /// Raw column bytes.
    data: Vec<u8>,
}

pub(crate) struct Value {
    /// A value may consist of multiple logical columns/fields.
    /// Each column is optional and may be absent within a value.
    columns: Vec<Option<Column>>,
}

impl Key {
    /// Creates a new `Key`.
    ///
    /// \- `group`: logical namespace / group id
    /// \- `data`: raw key bytes
    pub(crate) fn new(group: u16, data: Vec<u8>) -> Self {
        Self { group, data }
    }

    /// Returns the group identifier.
    pub(crate) fn group(&self) -> u16 {
        self.group
    }

    /// Returns the raw key bytes.
    pub(crate) fn data(&self) -> &[u8] {
        &self.data
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

    /// Merges this column with a newer column.
    ///
    /// Merge semantics:
    /// - If `newer` is `Put` or `Delete`, it replaces `self` entirely.
    /// - If `newer` is `Merge`, the data is concatenated to `self`'s data.
    ///
    /// Returns a new `Column` with the merged result.
    pub(crate) fn merge(&self, newer: &Column) -> Column {
        match newer.value_type {
            ValueType::Put | ValueType::Delete => newer.clone(),
            ValueType::Merge => {
                let mut merged_data = self.data.clone();
                merged_data.extend_from_slice(&newer.data);
                Column::new(self.value_type, merged_data)
            }
        }
    }
}

impl Value {
    /// Creates a new `Value` from a list of optional columns.
    pub(crate) fn new(columns: Vec<Option<Column>>) -> Self {
        Self { columns }
    }

    /// Returns the optional columns.
    pub(crate) fn columns(&self) -> &[Option<Column>] {
        &self.columns
    }

    /// Merges this value with a newer value.
    ///
    /// Columns at the same position are merged according to their types:
    /// - If the newer column is `None`, keep the older column.
    /// - If the newer column exists, merge it with the older column using `Column::merge`.
    ///
    /// The resulting value has the maximum number of columns from both values.
    pub(crate) fn merge(&self, newer: &Value) -> Value {
        let max_cols = self.columns.len().max(newer.columns.len());
        let mut merged_columns = Vec::with_capacity(max_cols);

        for i in 0..max_cols {
            let older_col = self.columns.get(i).and_then(|c| c.as_ref());
            let newer_col = newer.columns.get(i).and_then(|c| c.as_ref());

            let merged = match (older_col, newer_col) {
                (Some(old), Some(new)) => Some(old.merge(new)),
                (Some(old), None) => Some(old.clone()),
                (None, Some(new)) => Some(new.clone()),
                (None, None) => None,
            };
            merged_columns.push(merged);
        }

        Value::new(merged_columns)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_merge_with_put_replaces() {
        let old = Column::new(ValueType::Put, b"old_data".to_vec());
        let new = Column::new(ValueType::Put, b"new_data".to_vec());

        let merged = old.merge(&new);
        assert!(matches!(merged.value_type(), ValueType::Put));
        assert_eq!(merged.data(), b"new_data");
    }

    #[test]
    fn test_column_merge_with_delete_replaces() {
        let old = Column::new(ValueType::Put, b"old_data".to_vec());
        let new = Column::new(ValueType::Delete, b"".to_vec());

        let merged = old.merge(&new);
        assert!(matches!(merged.value_type(), ValueType::Delete));
        assert_eq!(merged.data(), b"");
    }

    #[test]
    fn test_column_merge_with_merge_concatenates() {
        let old = Column::new(ValueType::Put, b"hello".to_vec());
        let new = Column::new(ValueType::Merge, b"world".to_vec());

        let merged = old.merge(&new);
        // Merge keeps the original value_type and concatenates data
        assert!(matches!(merged.value_type(), ValueType::Put));
        assert_eq!(merged.data(), b"helloworld");
    }

    #[test]
    fn test_column_merge_multiple_merges() {
        let old = Column::new(ValueType::Put, b"a".to_vec());
        let merge1 = Column::new(ValueType::Merge, b"b".to_vec());
        let merge2 = Column::new(ValueType::Merge, b"c".to_vec());

        let merged = old.merge(&merge1).merge(&merge2);
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

        let merged = old.merge(&new);
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

        let merged = old.merge(&new);
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

        let merged = old.merge(&new);
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

        let merged = old.merge(&new);
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

        let merged = old.merge(&new);
        let cols = merged.columns();

        assert_eq!(cols.len(), 2);
        assert!(cols[0].is_none());
        assert!(cols[1].is_none());
    }
}
