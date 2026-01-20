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

pub(crate) struct Column {
    /// Write semantics of this column (Put/Delete/Merge).
    value_type: ValueType,

    /// Raw column bytes.
    data: Vec<u8>,
}

pub(crate) struct Value {
    /// A value may consist of multiple logical columns/fields.
    columns: Vec<Column>,
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
}

impl Value {
    /// Creates a new `Value` from a list of columns.
    pub(crate) fn new(columns: Vec<Column>) -> Self {
        Self { columns }
    }

    /// Returns the columns.
    pub(crate) fn columns(&self) -> &[Column] {
        &self.columns
    }
}
