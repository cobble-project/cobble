use crate::error::{Error, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};

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
    Put = 0,

    /// Tombstone semantics: marks a key/field as deleted.
    Delete = 1,

    /// Merge semantics: requires a merge operator during reads/compaction.
    Merge = 2,
}

impl ValueType {
    /// Encodes the value type to a single byte.
    pub(crate) fn encode(&self) -> u8 {
        *self as u8
    }

    /// Decodes a value type from a single byte.
    pub(crate) fn decode(byte: u8) -> Result<Self> {
        match byte {
            0 => Ok(ValueType::Put),
            1 => Ok(ValueType::Delete),
            2 => Ok(ValueType::Merge),
            _ => Err(Error::IoError(format!("Invalid ValueType: {}", byte))),
        }
    }
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

    /// Returns the encoded size in bytes.
    /// Layout: [group: u16][data_len: u32][data: bytes]
    pub(crate) fn encoded_size(&self) -> usize {
        2 + 4 + self.data.len()
    }

    /// Encodes the key to bytes.
    /// Layout: [group: u16][data_len: u32][data: bytes]
    pub(crate) fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(self.encoded_size());
        buf.put_u16_le(self.group);
        buf.put_u32_le(self.data.len() as u32);
        buf.put_slice(&self.data);
        buf.freeze()
    }

    /// Decodes a key from bytes.
    pub(crate) fn decode(data: &[u8]) -> Result<Self> {
        if data.len() < 6 {
            return Err(Error::IoError(format!(
                "Key data too small: expected at least 6 bytes, got {}",
                data.len()
            )));
        }

        let mut buf = data;
        let group = buf.get_u16_le();
        let data_len = buf.get_u32_le() as usize;

        if buf.remaining() < data_len {
            return Err(Error::IoError(format!(
                "Key data corrupted: expected {} bytes, got {}",
                data_len,
                buf.remaining()
            )));
        }

        let key_data = buf[..data_len].to_vec();
        Ok(Self {
            group,
            data: key_data,
        })
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

    /// Returns the encoded size in bytes.
    /// Layout: [value_type: u8][data_len: u32][data: bytes]
    pub(crate) fn encoded_size(&self) -> usize {
        1 + 4 + self.data.len()
    }

    /// Encodes the column to bytes.
    /// Layout: [value_type: u8][data_len: u32][data: bytes]
    pub(crate) fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(self.encoded_size());
        buf.put_u8(self.value_type.encode());
        buf.put_u32_le(self.data.len() as u32);
        buf.put_slice(&self.data);
        buf.freeze()
    }

    /// Decodes a column from bytes.
    pub(crate) fn decode(data: &[u8]) -> Result<Self> {
        let (column, _) = Self::decode_with_size(data)?;
        Ok(column)
    }

    /// Decodes a column from bytes and returns the number of bytes consumed.
    pub(crate) fn decode_with_size(data: &[u8]) -> Result<(Self, usize)> {
        if data.len() < 5 {
            return Err(Error::IoError(format!(
                "Column data too small: expected at least 5 bytes, got {}",
                data.len()
            )));
        }

        let mut buf = data;
        let value_type = ValueType::decode(buf.get_u8())?;
        let data_len = buf.get_u32_le() as usize;

        if buf.remaining() < data_len {
            return Err(Error::IoError(format!(
                "Column data corrupted: expected {} bytes, got {}",
                data_len,
                buf.remaining()
            )));
        }

        let column_data = buf[..data_len].to_vec();
        let consumed = 1 + 4 + data_len;
        Ok((
            Self {
                value_type,
                data: column_data,
            },
            consumed,
        ))
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

    /// Returns the encoded size in bytes.
    /// Layout: [num_columns: u32][columns...]
    pub(crate) fn encoded_size(&self) -> usize {
        4 + self.columns.iter().map(|c| c.encoded_size()).sum::<usize>()
    }

    /// Encodes the value to bytes.
    /// Layout: [num_columns: u32][columns...]
    pub(crate) fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(self.encoded_size());
        buf.put_u32_le(self.columns.len() as u32);
        for column in &self.columns {
            buf.put_slice(&column.encode());
        }
        buf.freeze()
    }

    /// Decodes a value from bytes.
    pub(crate) fn decode(data: &[u8]) -> Result<Self> {
        if data.len() < 4 {
            return Err(Error::IoError(format!(
                "Value data too small: expected at least 4 bytes, got {}",
                data.len()
            )));
        }

        let mut buf = data;
        let num_columns = buf.get_u32_le() as usize;

        let mut columns = Vec::with_capacity(num_columns);
        for _ in 0..num_columns {
            if buf.remaining() < 5 {
                return Err(Error::IoError(
                    "Value data corrupted: not enough bytes for column".to_string(),
                ));
            }

            let (column, consumed) = Column::decode_with_size(buf)?;
            buf = &buf[consumed..];
            columns.push(column);
        }

        Ok(Self { columns })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_encode_decode() {
        let key = Key::new(42, b"hello world".to_vec());
        let encoded = key.encode();

        // Verify encoded size: 2 (group) + 4 (data_len) + 11 (data) = 17
        assert_eq!(encoded.len(), 17);
        assert_eq!(key.encoded_size(), 17);

        let decoded = Key::decode(&encoded).unwrap();
        assert_eq!(decoded.group(), 42);
        assert_eq!(decoded.data(), b"hello world");
    }

    #[test]
    fn test_key_empty_data() {
        let key = Key::new(0, Vec::new());
        let encoded = key.encode();

        // Verify encoded size: 2 (group) + 4 (data_len) + 0 (data) = 6
        assert_eq!(encoded.len(), 6);

        let decoded = Key::decode(&encoded).unwrap();
        assert_eq!(decoded.group(), 0);
        assert_eq!(decoded.data(), b"");
    }

    #[test]
    fn test_key_max_group() {
        let key = Key::new(u16::MAX, b"test".to_vec());
        let encoded = key.encode();

        let decoded = Key::decode(&encoded).unwrap();
        assert_eq!(decoded.group(), u16::MAX);
        assert_eq!(decoded.data(), b"test");
    }

    #[test]
    fn test_key_decode_too_small() {
        let result = Key::decode(&[0, 1, 2, 3, 4]);
        assert!(result.is_err());
    }

    #[test]
    fn test_key_decode_corrupted() {
        // Create a buffer with data_len = 100 but only 4 bytes of actual data
        let mut buf = BytesMut::new();
        buf.put_u16_le(1);
        buf.put_u32_le(100); // claims 100 bytes
        buf.put_slice(b"test"); // only 4 bytes

        let result = Key::decode(&buf);
        assert!(result.is_err());
    }

    #[test]
    fn test_value_type_encode_decode() {
        assert_eq!(ValueType::Put.encode(), 0);
        assert_eq!(ValueType::Delete.encode(), 1);
        assert_eq!(ValueType::Merge.encode(), 2);

        assert!(matches!(ValueType::decode(0).unwrap(), ValueType::Put));
        assert!(matches!(ValueType::decode(1).unwrap(), ValueType::Delete));
        assert!(matches!(ValueType::decode(2).unwrap(), ValueType::Merge));
    }

    #[test]
    fn test_value_type_decode_invalid() {
        let result = ValueType::decode(3);
        assert!(result.is_err());

        let result = ValueType::decode(255);
        assert!(result.is_err());
    }

    #[test]
    fn test_column_encode_decode() {
        let column = Column::new(ValueType::Put, b"column data".to_vec());
        let encoded = column.encode();

        // Verify encoded size: 1 (value_type) + 4 (data_len) + 11 (data) = 16
        assert_eq!(encoded.len(), 16);
        assert_eq!(column.encoded_size(), 16);

        let decoded = Column::decode(&encoded).unwrap();
        assert!(matches!(decoded.value_type(), ValueType::Put));
        assert_eq!(decoded.data(), b"column data");
    }

    #[test]
    fn test_column_all_value_types() {
        for vt in [ValueType::Put, ValueType::Delete, ValueType::Merge] {
            let column = Column::new(vt, b"test".to_vec());
            let encoded = column.encode();
            let decoded = Column::decode(&encoded).unwrap();
            assert_eq!(decoded.value_type().encode(), column.value_type().encode());
            assert_eq!(decoded.data(), b"test");
        }
    }

    #[test]
    fn test_column_empty_data() {
        let column = Column::new(ValueType::Delete, Vec::new());
        let encoded = column.encode();

        // Verify encoded size: 1 (value_type) + 4 (data_len) + 0 (data) = 5
        assert_eq!(encoded.len(), 5);

        let decoded = Column::decode(&encoded).unwrap();
        assert!(matches!(decoded.value_type(), ValueType::Delete));
        assert_eq!(decoded.data(), b"");
    }

    #[test]
    fn test_column_decode_too_small() {
        let result = Column::decode(&[0, 1, 2, 3]);
        assert!(result.is_err());
    }

    #[test]
    fn test_value_encode_decode_single_column() {
        let value = Value::new(vec![Column::new(ValueType::Put, b"data1".to_vec())]);
        let encoded = value.encode();

        // Verify encoded size: 4 (num_columns) + 1 + 4 + 5 = 14
        assert_eq!(encoded.len(), 14);
        assert_eq!(value.encoded_size(), 14);

        let decoded = Value::decode(&encoded).unwrap();
        assert_eq!(decoded.columns().len(), 1);
        assert!(matches!(decoded.columns()[0].value_type(), ValueType::Put));
        assert_eq!(decoded.columns()[0].data(), b"data1");
    }

    #[test]
    fn test_value_encode_decode_multiple_columns() {
        let value = Value::new(vec![
            Column::new(ValueType::Put, b"col1".to_vec()),
            Column::new(ValueType::Delete, b"col2_longer".to_vec()),
            Column::new(ValueType::Merge, b"c3".to_vec()),
        ]);
        let encoded = value.encode();
        let decoded = Value::decode(&encoded).unwrap();

        assert_eq!(decoded.columns().len(), 3);

        assert!(matches!(decoded.columns()[0].value_type(), ValueType::Put));
        assert_eq!(decoded.columns()[0].data(), b"col1");

        assert!(matches!(
            decoded.columns()[1].value_type(),
            ValueType::Delete
        ));
        assert_eq!(decoded.columns()[1].data(), b"col2_longer");

        assert!(matches!(
            decoded.columns()[2].value_type(),
            ValueType::Merge
        ));
        assert_eq!(decoded.columns()[2].data(), b"c3");
    }

    #[test]
    fn test_value_encode_decode_empty() {
        let value = Value::new(Vec::new());
        let encoded = value.encode();

        // Verify encoded size: 4 (num_columns) = 4
        assert_eq!(encoded.len(), 4);

        let decoded = Value::decode(&encoded).unwrap();
        assert_eq!(decoded.columns().len(), 0);
    }

    #[test]
    fn test_value_decode_too_small() {
        let result = Value::decode(&[0, 1, 2]);
        assert!(result.is_err());
    }

    #[test]
    fn test_value_decode_corrupted_column_count() {
        // Claims 2 columns but only has 1
        let mut buf = BytesMut::new();
        buf.put_u32_le(2); // claims 2 columns
        buf.put_u8(0); // value_type
        buf.put_u32_le(4); // data_len
        buf.put_slice(b"test"); // one complete column

        let result = Value::decode(&buf);
        assert!(result.is_err());
    }

    #[test]
    fn test_large_data() {
        // Test with larger data to ensure the codec works correctly
        let large_data: Vec<u8> = (0..10000).map(|i| (i % 256) as u8).collect();

        let key = Key::new(1234, large_data.clone());
        let encoded_key = key.encode();
        let decoded_key = Key::decode(&encoded_key).unwrap();
        assert_eq!(decoded_key.group(), 1234);
        assert_eq!(decoded_key.data(), large_data.as_slice());

        let column = Column::new(ValueType::Merge, large_data.clone());
        let encoded_column = column.encode();
        let decoded_column = Column::decode(&encoded_column).unwrap();
        assert_eq!(decoded_column.data(), large_data.as_slice());

        let value = Value::new(vec![
            Column::new(ValueType::Put, large_data.clone()),
            Column::new(ValueType::Delete, large_data.clone()),
        ]);
        let encoded_value = value.encode();
        let decoded_value = Value::decode(&encoded_value).unwrap();
        assert_eq!(decoded_value.columns().len(), 2);
        assert_eq!(decoded_value.columns()[0].data(), large_data.as_slice());
        assert_eq!(decoded_value.columns()[1].data(), large_data.as_slice());
    }

    #[test]
    fn test_binary_data_with_nulls() {
        // Test with binary data containing null bytes
        let binary_data = vec![0u8, 1, 0, 255, 0, 128, 0];

        let key = Key::new(100, binary_data.clone());
        let encoded = key.encode();
        let decoded = Key::decode(&encoded).unwrap();
        assert_eq!(decoded.data(), binary_data.as_slice());
    }
}
