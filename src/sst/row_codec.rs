//! Row codec for SST files.
//!
//! This module provides efficient binary serialization for Key and Value types
//! in row-based SST storage format.
//!
//! ## Key Format
//! ```text
//! [group: u16][data_len: u32][data: bytes]
//! ```
//!
//! ## Value Format (with optional columns)
//! The number of columns is stored in SST metadata, not in each value.
//! Each column can be optional (absent) within a value.
//! ```text
//! [presence_bitmap: variable][columns...]
//! ```
//!
//! Where:
//! - `presence_bitmap`: A bitmap where bit i indicates if column i is present.
//!   The bitmap size is `ceil(num_columns / 8)` bytes.
//! - Each present column: `[value_type: u8][data_len: u32][data: bytes]`

use crate::error::{Error, Result};
use crate::r#type::{Column, Key, Value, ValueType};
use bytes::{Buf, BufMut, Bytes, BytesMut};

/// Encodes a ValueType to a single byte.
pub(crate) fn encode_value_type(vt: &ValueType) -> u8 {
    match vt {
        ValueType::Put => 0,
        ValueType::Delete => 1,
        ValueType::Merge => 2,
    }
}

/// Decodes a ValueType from a single byte.
pub(crate) fn decode_value_type(byte: u8) -> Result<ValueType> {
    match byte {
        0 => Ok(ValueType::Put),
        1 => Ok(ValueType::Delete),
        2 => Ok(ValueType::Merge),
        _ => Err(Error::IoError(format!("Invalid ValueType: {}", byte))),
    }
}

/// Encodes a Key to bytes.
///
/// Layout: `[group: u16][data_len: u32][data: bytes]`
pub(crate) fn encode_key(key: &Key) -> Bytes {
    let size = 2 + 4 + key.data().len();
    let mut buf = BytesMut::with_capacity(size);
    buf.put_u16_le(key.group());
    buf.put_u32_le(key.data().len() as u32);
    buf.put_slice(key.data());
    buf.freeze()
}

/// Decodes a Key from bytes.
pub(crate) fn decode_key(data: &[u8]) -> Result<Key> {
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
    Ok(Key::new(group, key_data))
}

/// Returns the encoded size of a Key in bytes.
pub(crate) fn key_encoded_size(key: &Key) -> usize {
    2 + 4 + key.data().len()
}

/// Returns the size of the presence bitmap for the given number of columns.
///
/// Calculates `ceil(num_columns / 8)` to determine the number of bytes needed
/// for the bitmap, where each bit represents the presence of one column.
///
/// Optimization: When `num_columns == 1`, returns 0 since the bitmap is skipped.
/// A single-column value with no column present would be invalid anyway.
fn bitmap_size(num_columns: usize) -> usize {
    if num_columns <= 1 {
        0
    } else {
        (num_columns + 7) / 8
    }
}

/// Encodes a Value to bytes with optional columns.
///
/// The number of columns is provided by the caller (from SST metadata).
/// Layout: `[presence_bitmap][present_columns...]`
///
/// - `presence_bitmap`: A bitmap indicating which columns are present.
///   (Omitted when `num_columns == 1` as a single absent column means invalid value)
/// - Each present column: `[value_type: u8][data_len: u32][data: bytes]`
///
/// # Arguments
/// * `value` - The value to encode. If a column slot is `None`, it is absent.
/// * `num_columns` - Total number of columns (from SST metadata).
pub(crate) fn encode_value(columns: &[Option<&Column>], num_columns: usize) -> Bytes {
    let bmp_size = bitmap_size(num_columns);

    // Calculate total size
    let mut total_size = bmp_size;
    for col_opt in columns.iter().take(num_columns) {
        if let Some(col) = col_opt {
            total_size += 1 + 4 + col.data().len(); // value_type + data_len + data
        }
    }

    let mut buf = BytesMut::with_capacity(total_size);

    // Write presence bitmap (only if num_columns > 1)
    if bmp_size > 0 {
        let mut bitmap = vec![0u8; bmp_size];
        for (i, col_opt) in columns.iter().take(num_columns).enumerate() {
            if col_opt.is_some() {
                bitmap[i / 8] |= 1 << (i % 8);
            }
        }
        buf.put_slice(&bitmap);
    }

    // Write present columns
    for col_opt in columns.iter().take(num_columns) {
        if let Some(col) = col_opt {
            buf.put_u8(encode_value_type(col.value_type()));
            buf.put_u32_le(col.data().len() as u32);
            buf.put_slice(col.data());
        }
    }

    buf.freeze()
}

/// Decodes a Value from bytes with optional columns.
///
/// The number of columns is provided by the caller (from SST metadata).
///
/// # Arguments
/// * `data` - The encoded value bytes.
/// * `num_columns` - Total number of columns (from SST metadata).
///
/// # Returns
/// A vector of optional columns, where `None` indicates an absent column.
pub(crate) fn decode_value(data: &[u8], num_columns: usize) -> Result<Vec<Option<Column>>> {
    let bmp_size = bitmap_size(num_columns);

    if data.len() < bmp_size {
        return Err(Error::IoError(format!(
            "Value data too small: expected at least {} bytes for bitmap, got {}",
            bmp_size,
            data.len()
        )));
    }

    let bitmap = &data[..bmp_size];
    let mut buf = &data[bmp_size..];

    let mut columns = Vec::with_capacity(num_columns);
    for i in 0..num_columns {
        // When num_columns == 1, bitmap is skipped and the single column is always present
        let is_present = if num_columns == 1 {
            true
        } else {
            (bitmap[i / 8] >> (i % 8)) & 1 == 1
        };
        if is_present {
            if buf.remaining() < 5 {
                return Err(Error::IoError(format!(
                    "Column {} data corrupted: not enough bytes",
                    i
                )));
            }

            let value_type = decode_value_type(buf.get_u8())?;
            let data_len = buf.get_u32_le() as usize;

            if buf.remaining() < data_len {
                return Err(Error::IoError(format!(
                    "Column {} data corrupted: expected {} bytes, got {}",
                    i,
                    data_len,
                    buf.remaining()
                )));
            }

            let col_data = buf[..data_len].to_vec();
            buf = &buf[data_len..];
            columns.push(Some(Column::new(value_type, col_data)));
        } else {
            columns.push(None);
        }
    }

    Ok(columns)
}

/// Returns the encoded size of a Value in bytes.
///
/// # Arguments
/// * `columns` - The columns (optional) to encode.
/// * `num_columns` - Total number of columns (from SST metadata).
pub(crate) fn value_encoded_size(columns: &[Option<&Column>], num_columns: usize) -> usize {
    let bmp_size = bitmap_size(num_columns);
    let mut size = bmp_size;
    for col_opt in columns.iter().take(num_columns) {
        if let Some(col) = col_opt {
            size += 1 + 4 + col.data().len();
        }
    }
    size
}

/// Helper to convert a Value with all present columns to optional column references.
pub(crate) fn value_to_optional_refs(value: &Value) -> Vec<Option<&Column>> {
    value.columns().iter().map(|c| Some(c)).collect()
}

/// Helper to convert optional columns to a Value (filtering out None values).
pub(crate) fn optional_columns_to_value(columns: Vec<Option<Column>>) -> Value {
    let present_columns: Vec<Column> = columns.into_iter().flatten().collect();
    Value::new(present_columns)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_key() {
        let key = Key::new(42, b"hello world".to_vec());
        let encoded = encode_key(&key);

        // Verify encoded size: 2 (group) + 4 (data_len) + 11 (data) = 17
        assert_eq!(encoded.len(), 17);
        assert_eq!(key_encoded_size(&key), 17);

        let decoded = decode_key(&encoded).unwrap();
        assert_eq!(decoded.group(), 42);
        assert_eq!(decoded.data(), b"hello world");
    }

    #[test]
    fn test_key_empty_data() {
        let key = Key::new(0, Vec::new());
        let encoded = encode_key(&key);

        assert_eq!(encoded.len(), 6);

        let decoded = decode_key(&encoded).unwrap();
        assert_eq!(decoded.group(), 0);
        assert_eq!(decoded.data(), b"");
    }

    #[test]
    fn test_key_max_group() {
        let key = Key::new(u16::MAX, b"test".to_vec());
        let encoded = encode_key(&key);

        let decoded = decode_key(&encoded).unwrap();
        assert_eq!(decoded.group(), u16::MAX);
        assert_eq!(decoded.data(), b"test");
    }

    #[test]
    fn test_key_decode_too_small() {
        let result = decode_key(&[0, 1, 2, 3, 4]);
        assert!(result.is_err());
    }

    #[test]
    fn test_value_type_encode_decode() {
        assert_eq!(encode_value_type(&ValueType::Put), 0);
        assert_eq!(encode_value_type(&ValueType::Delete), 1);
        assert_eq!(encode_value_type(&ValueType::Merge), 2);

        assert!(matches!(decode_value_type(0).unwrap(), ValueType::Put));
        assert!(matches!(decode_value_type(1).unwrap(), ValueType::Delete));
        assert!(matches!(decode_value_type(2).unwrap(), ValueType::Merge));
    }

    #[test]
    fn test_value_type_decode_invalid() {
        assert!(decode_value_type(3).is_err());
        assert!(decode_value_type(255).is_err());
    }

    #[test]
    fn test_encode_decode_value_all_present() {
        let col1 = Column::new(ValueType::Put, b"data1".to_vec());
        let col2 = Column::new(ValueType::Delete, b"data2".to_vec());
        let columns: Vec<Option<&Column>> = vec![Some(&col1), Some(&col2)];

        let encoded = encode_value(&columns, 2);

        // Bitmap size: 1 byte for 2 columns
        // Column 1: 1 + 4 + 5 = 10
        // Column 2: 1 + 4 + 5 = 10
        // Total: 1 + 10 + 10 = 21
        assert_eq!(encoded.len(), 21);
        assert_eq!(value_encoded_size(&columns, 2), 21);

        let decoded = decode_value(&encoded, 2).unwrap();
        assert_eq!(decoded.len(), 2);

        assert!(decoded[0].is_some());
        let c0 = decoded[0].as_ref().unwrap();
        assert!(matches!(c0.value_type(), ValueType::Put));
        assert_eq!(c0.data(), b"data1");

        assert!(decoded[1].is_some());
        let c1 = decoded[1].as_ref().unwrap();
        assert!(matches!(c1.value_type(), ValueType::Delete));
        assert_eq!(c1.data(), b"data2");
    }

    #[test]
    fn test_encode_decode_value_with_optional() {
        let col1 = Column::new(ValueType::Put, b"present".to_vec());
        let columns: Vec<Option<&Column>> = vec![Some(&col1), None, None];

        let encoded = encode_value(&columns, 3);

        // Bitmap size: 1 byte for 3 columns
        // Only column 0 is present: 1 + 4 + 7 = 12
        // Total: 1 + 12 = 13
        assert_eq!(encoded.len(), 13);

        let decoded = decode_value(&encoded, 3).unwrap();
        assert_eq!(decoded.len(), 3);

        assert!(decoded[0].is_some());
        assert_eq!(decoded[0].as_ref().unwrap().data(), b"present");

        assert!(decoded[1].is_none());
        assert!(decoded[2].is_none());
    }

    #[test]
    fn test_encode_decode_value_all_absent() {
        let columns: Vec<Option<&Column>> = vec![None, None, None, None];

        let encoded = encode_value(&columns, 4);

        // Bitmap size: 1 byte for 4 columns, no column data
        assert_eq!(encoded.len(), 1);

        let decoded = decode_value(&encoded, 4).unwrap();
        assert_eq!(decoded.len(), 4);
        assert!(decoded.iter().all(|c| c.is_none()));
    }

    #[test]
    fn test_encode_decode_value_many_columns() {
        // Test with 16 columns (2 bytes bitmap)
        let col = Column::new(ValueType::Merge, b"x".to_vec());
        let mut columns: Vec<Option<&Column>> = vec![None; 16];
        columns[0] = Some(&col);
        columns[8] = Some(&col);
        columns[15] = Some(&col);

        let encoded = encode_value(&columns, 16);

        // Bitmap size: 2 bytes for 16 columns
        // 3 columns present: 3 * (1 + 4 + 1) = 18
        // Total: 2 + 18 = 20
        assert_eq!(encoded.len(), 20);

        let decoded = decode_value(&encoded, 16).unwrap();
        assert_eq!(decoded.len(), 16);

        assert!(decoded[0].is_some());
        assert!(decoded[1].is_none());
        assert!(decoded[8].is_some());
        assert!(decoded[15].is_some());
    }

    #[test]
    fn test_bitmap_size() {
        assert_eq!(bitmap_size(0), 0);
        assert_eq!(bitmap_size(1), 0); // Optimized: no bitmap for single column
        assert_eq!(bitmap_size(2), 1);
        assert_eq!(bitmap_size(8), 1);
        assert_eq!(bitmap_size(9), 2);
        assert_eq!(bitmap_size(16), 2);
        assert_eq!(bitmap_size(17), 3);
    }

    #[test]
    fn test_value_decode_too_small() {
        // For 2 columns, need at least 1 byte bitmap
        let result = decode_value(&[], 2);
        assert!(result.is_err());
    }

    #[test]
    fn test_single_column_no_bitmap() {
        // Single column optimization: no bitmap, column must be present
        let col = Column::new(ValueType::Put, b"single".to_vec());
        let columns: Vec<Option<&Column>> = vec![Some(&col)];

        let encoded = encode_value(&columns, 1);

        // No bitmap for single column: just the column data
        // 1 (value_type) + 4 (data_len) + 6 (data) = 11
        assert_eq!(encoded.len(), 11);
        assert_eq!(value_encoded_size(&columns, 1), 11);

        let decoded = decode_value(&encoded, 1).unwrap();
        assert_eq!(decoded.len(), 1);
        assert!(decoded[0].is_some());
        assert_eq!(decoded[0].as_ref().unwrap().data(), b"single");
    }

    #[test]
    fn test_value_to_optional_refs() {
        let value = Value::new(vec![
            Column::new(ValueType::Put, b"a".to_vec()),
            Column::new(ValueType::Delete, b"b".to_vec()),
        ]);

        let refs = value_to_optional_refs(&value);
        assert_eq!(refs.len(), 2);
        assert!(refs[0].is_some());
        assert!(refs[1].is_some());
    }

    #[test]
    fn test_optional_columns_to_value() {
        let columns = vec![
            Some(Column::new(ValueType::Put, b"a".to_vec())),
            None,
            Some(Column::new(ValueType::Merge, b"c".to_vec())),
        ];

        let value = optional_columns_to_value(columns);
        // Only present columns are kept
        assert_eq!(value.columns().len(), 2);
    }

    #[test]
    fn test_large_data() {
        let large_data: Vec<u8> = (0..10000).map(|i| (i % 256) as u8).collect();

        let key = Key::new(1234, large_data.clone());
        let encoded_key = encode_key(&key);
        let decoded_key = decode_key(&encoded_key).unwrap();
        assert_eq!(decoded_key.group(), 1234);
        assert_eq!(decoded_key.data(), large_data.as_slice());

        let col = Column::new(ValueType::Put, large_data.clone());
        let columns: Vec<Option<&Column>> = vec![Some(&col)];
        let encoded = encode_value(&columns, 1);
        let decoded = decode_value(&encoded, 1).unwrap();
        assert!(decoded[0].is_some());
        assert_eq!(decoded[0].as_ref().unwrap().data(), large_data.as_slice());
    }

    #[test]
    fn test_binary_data_with_nulls() {
        let binary_data = vec![0u8, 1, 0, 255, 0, 128, 0];

        let key = Key::new(100, binary_data.clone());
        let encoded = encode_key(&key);
        let decoded = decode_key(&encoded).unwrap();
        assert_eq!(decoded.data(), binary_data.as_slice());
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_sst_key_value_codec() {
        use crate::file::FileSystemRegistry;
        use crate::sst::writer::{SSTWriter, SSTWriterOptions};
        use crate::sst::iterator::{SSTIterator, SSTIteratorOptions};

        let _ = std::fs::remove_dir_all("/tmp/sst_row_codec_test");
        let registry = FileSystemRegistry::new();
        let fs = registry
            .get_or_register("file:///tmp/sst_row_codec_test".to_string())
            .unwrap();

        // Define schema: 2 columns (name, email)
        let num_columns = 2;

        // Create test Key and Value using the codec
        let key1 = Key::new(1, b"user:1".to_vec());
        let col1_name = Column::new(ValueType::Put, b"Alice".to_vec());
        let col1_email = Column::new(ValueType::Put, b"alice@example.com".to_vec());
        let value1_cols: Vec<Option<&Column>> = vec![Some(&col1_name), Some(&col1_email)];

        let key2 = Key::new(1, b"user:2".to_vec());
        let col2_name = Column::new(ValueType::Put, b"Bob".to_vec());
        // user:2 has no email (optional column)
        let value2_cols: Vec<Option<&Column>> = vec![Some(&col2_name), None];

        let key3 = Key::new(2, b"order:100".to_vec());
        // order:100 is deleted (all columns absent)
        let value3_cols: Vec<Option<&Column>> = vec![None, None];

        // Write SST file with encoded Key/Value
        {
            let writer_file = fs.open_write("codec_test.sst").unwrap();
            let mut writer = SSTWriter::new(writer_file, SSTWriterOptions::default());

            writer.add(&encode_key(&key1), &encode_value(&value1_cols, num_columns)).unwrap();
            writer.add(&encode_key(&key2), &encode_value(&value2_cols, num_columns)).unwrap();
            writer.add(&encode_key(&key3), &encode_value(&value3_cols, num_columns)).unwrap();

            writer.finish().unwrap();
        }

        // Read SST file and decode Key/Value
        {
            let reader_file = fs.open_read("codec_test.sst").unwrap();
            let mut iter = SSTIterator::new(reader_file, SSTIteratorOptions::default()).unwrap();

            iter.seek_to_first().unwrap();

            // First entry: user:1 with name="Alice", email="alice@example.com"
            assert!(iter.valid());
            let (key_bytes, value_bytes) = iter.current().unwrap().unwrap();
            let decoded_key = decode_key(&key_bytes).unwrap();
            let decoded_cols = decode_value(&value_bytes, num_columns).unwrap();

            assert_eq!(decoded_key.group(), 1);
            assert_eq!(decoded_key.data(), b"user:1");
            assert_eq!(decoded_cols.len(), 2);
            assert!(decoded_cols[0].is_some());
            assert_eq!(decoded_cols[0].as_ref().unwrap().data(), b"Alice");
            assert!(decoded_cols[1].is_some());
            assert_eq!(decoded_cols[1].as_ref().unwrap().data(), b"alice@example.com");

            // Second entry: user:2 with name="Bob", email=None
            iter.next().unwrap();
            assert!(iter.valid());
            let (key_bytes, value_bytes) = iter.current().unwrap().unwrap();
            let decoded_key = decode_key(&key_bytes).unwrap();
            let decoded_cols = decode_value(&value_bytes, num_columns).unwrap();

            assert_eq!(decoded_key.group(), 1);
            assert_eq!(decoded_key.data(), b"user:2");
            assert!(decoded_cols[0].is_some());
            assert_eq!(decoded_cols[0].as_ref().unwrap().data(), b"Bob");
            assert!(decoded_cols[1].is_none());

            // Third entry: order:100 with all columns absent
            iter.next().unwrap();
            assert!(iter.valid());
            let (key_bytes, value_bytes) = iter.current().unwrap().unwrap();
            let decoded_key = decode_key(&key_bytes).unwrap();
            let decoded_cols = decode_value(&value_bytes, num_columns).unwrap();

            assert_eq!(decoded_key.group(), 2);
            assert_eq!(decoded_key.data(), b"order:100");
            assert!(decoded_cols[0].is_none());
            assert!(decoded_cols[1].is_none());

            // No more entries
            iter.next().unwrap();
            assert!(!iter.valid());
        }

        let _ = std::fs::remove_dir_all("/tmp/sst_row_codec_test");
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_sst_key_value_codec_seek() {
        use crate::file::FileSystemRegistry;
        use crate::sst::writer::{SSTWriter, SSTWriterOptions};
        use crate::sst::iterator::{SSTIterator, SSTIteratorOptions};

        let _ = std::fs::remove_dir_all("/tmp/sst_row_codec_seek_test");
        let registry = FileSystemRegistry::new();
        let fs = registry
            .get_or_register("file:///tmp/sst_row_codec_seek_test".to_string())
            .unwrap();

        let num_columns = 1;

        let key1 = Key::new(1, b"aaa".to_vec());
        let key2 = Key::new(1, b"bbb".to_vec());
        let key3 = Key::new(2, b"aaa".to_vec());

        let col = Column::new(ValueType::Put, b"test".to_vec());
        let value_cols: Vec<Option<&Column>> = vec![Some(&col)];

        // Write SST file
        {
            let writer_file = fs.open_write("codec_seek_test.sst").unwrap();
            let mut writer = SSTWriter::new(writer_file, SSTWriterOptions::default());

            writer.add(&encode_key(&key1), &encode_value(&value_cols, num_columns)).unwrap();
            writer.add(&encode_key(&key2), &encode_value(&value_cols, num_columns)).unwrap();
            writer.add(&encode_key(&key3), &encode_value(&value_cols, num_columns)).unwrap();

            writer.finish().unwrap();
        }

        // Read and seek using encoded key
        {
            let reader_file = fs.open_read("codec_seek_test.sst").unwrap();
            let mut iter = SSTIterator::new(reader_file, SSTIteratorOptions::default()).unwrap();

            // Seek to second key
            let seek_key = Key::new(1, b"bbb".to_vec());
            iter.seek(&encode_key(&seek_key)).unwrap();
            assert!(iter.valid());

            let (key_bytes, _) = iter.current().unwrap().unwrap();
            let decoded_key = decode_key(&key_bytes).unwrap();
            assert_eq!(decoded_key.group(), 1);
            assert_eq!(decoded_key.data(), b"bbb");
        }

        let _ = std::fs::remove_dir_all("/tmp/sst_row_codec_seek_test");
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_sst_key_value_codec_multiple_blocks() {
        use crate::file::FileSystemRegistry;
        use crate::sst::writer::{SSTWriter, SSTWriterOptions};
        use crate::sst::iterator::{SSTIterator, SSTIteratorOptions};

        let _ = std::fs::remove_dir_all("/tmp/sst_row_codec_blocks_test");
        let registry = FileSystemRegistry::new();
        let fs = registry
            .get_or_register("file:///tmp/sst_row_codec_blocks_test".to_string())
            .unwrap();

        let num_columns = 2;
        let num_entries = 50;

        // Write SST file with many entries across multiple blocks
        {
            let writer_file = fs.open_write("codec_blocks_test.sst").unwrap();
            let mut writer = SSTWriter::new(
                writer_file,
                SSTWriterOptions {
                    block_size: 200, // Small block size to force multiple blocks
                    buffer_size: 8192,
                },
            );

            for i in 0..num_entries {
                let key = Key::new(i as u16, format!("key{:04}", i).into_bytes());
                let col1 = Column::new(ValueType::Put, format!("val{:04}", i).into_bytes());
                let col2 = Column::new(ValueType::Merge, b"extra".to_vec());
                // Alternate: some entries have second column, some don't
                let cols: Vec<Option<&Column>> = if i % 2 == 0 {
                    vec![Some(&col1), Some(&col2)]
                } else {
                    vec![Some(&col1), None]
                };
                writer.add(&encode_key(&key), &encode_value(&cols, num_columns)).unwrap();
            }

            writer.finish().unwrap();
        }

        // Read and verify all entries
        {
            let reader_file = fs.open_read("codec_blocks_test.sst").unwrap();
            let mut iter = SSTIterator::new(reader_file, SSTIteratorOptions::default()).unwrap();

            iter.seek_to_first().unwrap();

            let mut count = 0;
            while iter.valid() {
                let (key_bytes, value_bytes) = iter.current().unwrap().unwrap();
                let decoded_key = decode_key(&key_bytes).unwrap();
                let decoded_cols = decode_value(&value_bytes, num_columns).unwrap();

                assert_eq!(decoded_key.group(), count as u16);
                assert_eq!(decoded_key.data(), format!("key{:04}", count).as_bytes());

                assert!(decoded_cols[0].is_some());
                assert_eq!(
                    decoded_cols[0].as_ref().unwrap().data(),
                    format!("val{:04}", count).as_bytes()
                );

                // Even entries have second column
                if count % 2 == 0 {
                    assert!(decoded_cols[1].is_some());
                } else {
                    assert!(decoded_cols[1].is_none());
                }

                count += 1;
                iter.next().unwrap();
            }

            assert_eq!(count, num_entries);
        }

        let _ = std::fs::remove_dir_all("/tmp/sst_row_codec_blocks_test");
    }
}
