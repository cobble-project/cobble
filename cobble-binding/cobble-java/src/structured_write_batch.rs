use crate::structured::db_from_handle;
use crate::structured_read_options::structured_read_options_from_handle_or_throw;
use crate::structured_write_options::structured_write_options_from_handle_or_throw;
use crate::util::{decode_u16, throw_illegal_argument, throw_illegal_state};
use cobble_binding::structured::ffi as structured_ffi;
use cobble_binding::structured::{StructuredColumnValue, StructuredWriteOptions};
use jni::JNIEnv;
use jni::objects::{JClass, JIntArray, JLongArray};
use jni::sys::{jint, jlong};
use std::borrow::Cow;

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_Db_putBytesDirectBatchWithOptions(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    bucket: jint,
    column: jint,
    encoded_address: jlong,
    encoded_capacity: jint,
    encoded_length: jint,
    entry_count: jint,
    write_options_handle: jlong,
) {
    let Some(db) = db_from_handle(&mut env, handle) else {
        return;
    };
    let bucket = match decode_u16("bucket", bucket) {
        Ok(value) => value,
        Err(error) => {
            throw_illegal_argument(&mut env, error);
            return;
        }
    };
    let column = match decode_u16("column", column) {
        Ok(value) => value,
        Err(error) => {
            throw_illegal_argument(&mut env, error);
            return;
        }
    };
    let Some((encoded_address, encoded_length, entry_count)) = decode_batch_bounds(
        &mut env,
        encoded_address,
        encoded_capacity,
        encoded_length,
        entry_count,
    ) else {
        return;
    };
    let encoded = unsafe { std::slice::from_raw_parts(encoded_address, encoded_length) };
    let default_options = StructuredWriteOptions::default();
    let options = if write_options_handle == 0 {
        &default_options
    } else {
        let Some(handle) =
            structured_write_options_from_handle_or_throw(&mut env, write_options_handle)
        else {
            return;
        };
        handle.write_options()
    };

    if let Err(error) = write_batch(db, bucket, column, encoded, entry_count, options) {
        throw_illegal_state(&mut env, error);
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_Db_putBytesDirectChunksWithOptions(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    bucket: jint,
    column: jint,
    chunk_addresses: JLongArray,
    chunk_lengths: JIntArray,
    key_lengths: JIntArray,
    value_lengths: JIntArray,
    entry_count: jint,
    write_options_handle: jlong,
) {
    let Some(db) = db_from_handle(&mut env, handle) else {
        return;
    };
    let bucket = match decode_u16("bucket", bucket) {
        Ok(value) => value,
        Err(error) => {
            throw_illegal_argument(&mut env, error);
            return;
        }
    };
    let column = match decode_u16("column", column) {
        Ok(value) => value,
        Err(error) => {
            throw_illegal_argument(&mut env, error);
            return;
        }
    };
    let chunks = match decode_direct_chunks(&mut env, &chunk_addresses, &chunk_lengths) {
        Ok(value) => value,
        Err(error) => {
            throw_illegal_argument(&mut env, error);
            return;
        }
    };
    let entry_count = match decode_count("entryCount", entry_count) {
        Ok(value) => value,
        Err(error) => {
            throw_illegal_argument(&mut env, error);
            return;
        }
    };
    let key_lengths =
        match decode_non_negative_lengths(&mut env, &key_lengths, "keyLengths", entry_count) {
            Ok(value) => value,
            Err(error) => {
                throw_illegal_argument(&mut env, error);
                return;
            }
        };
    let value_lengths =
        match decode_non_negative_lengths(&mut env, &value_lengths, "valueLengths", entry_count) {
            Ok(value) => value,
            Err(error) => {
                throw_illegal_argument(&mut env, error);
                return;
            }
        };
    if key_lengths.len() != value_lengths.len() {
        throw_illegal_argument(
            &mut env,
            "keyLengths and valueLengths differ in size".to_string(),
        );
        return;
    }

    let mut cursor = DirectChunksCursor::new(chunks);
    let mut entries = Vec::with_capacity(key_lengths.len());
    for (key_length, value_length) in key_lengths.into_iter().zip(value_lengths) {
        let key = match cursor.take(key_length) {
            Ok(value) => value,
            Err(error) => {
                throw_illegal_argument(&mut env, error);
                return;
            }
        };
        let value = match cursor.take(value_length) {
            Ok(value) => value,
            Err(error) => {
                throw_illegal_argument(&mut env, error);
                return;
            }
        };
        entries.push((key, value));
    }
    if let Err(error) = cursor.ensure_exhausted() {
        throw_illegal_argument(&mut env, error);
        return;
    }

    let default_options = StructuredWriteOptions::default();
    let options = if write_options_handle == 0 {
        &default_options
    } else {
        let Some(handle) =
            structured_write_options_from_handle_or_throw(&mut env, write_options_handle)
        else {
            return;
        };
        handle.write_options()
    };
    let borrowed = entries
        .iter()
        .map(|(key, value)| (key.as_ref(), value.as_ref()));
    if let Err(error) =
        structured_ffi::db_put_bytes_batch_with_options(db, bucket, column, borrowed, options)
    {
        throw_illegal_state(&mut env, error.to_string());
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_cobble_structured_Db_multiGetBytesDirectChunksWithOptions(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    chunk_addresses: JLongArray,
    chunk_lengths: JIntArray,
    buckets: JIntArray,
    key_lengths: JIntArray,
    key_count: jint,
    result_address: jlong,
    result_capacity: jint,
    read_options_handle: jlong,
) -> jint {
    let Some(db) = db_from_handle(&mut env, handle) else {
        return 0;
    };
    let chunks = match decode_direct_chunks(&mut env, &chunk_addresses, &chunk_lengths) {
        Ok(value) => value,
        Err(error) => {
            throw_illegal_argument(&mut env, error);
            return 0;
        }
    };
    let key_count = match decode_count("keyCount", key_count) {
        Ok(value) => value,
        Err(error) => {
            throw_illegal_argument(&mut env, error);
            return 0;
        }
    };
    let key_lengths =
        match decode_non_negative_lengths(&mut env, &key_lengths, "keyLengths", key_count) {
            Ok(value) => value,
            Err(error) => {
                throw_illegal_argument(&mut env, error);
                return 0;
            }
        };
    let buckets = match decode_buckets(&mut env, &buckets, key_count) {
        Ok(value) => value,
        Err(error) => {
            throw_illegal_argument(&mut env, error);
            return 0;
        }
    };
    if buckets.len() != key_lengths.len() {
        throw_illegal_argument(
            &mut env,
            "buckets and keyLengths differ in size".to_string(),
        );
        return 0;
    }

    let mut cursor = DirectChunksCursor::new(chunks);
    let mut keys = Vec::with_capacity(key_lengths.len());
    for (bucket, key_length) in buckets.into_iter().zip(key_lengths) {
        let key = match cursor.take(key_length) {
            Ok(value) => value,
            Err(error) => {
                throw_illegal_argument(&mut env, error);
                return 0;
            }
        };
        keys.push((bucket, key));
    }
    if let Err(error) = cursor.ensure_exhausted() {
        throw_illegal_argument(&mut env, error);
        return 0;
    }
    let borrowed = keys
        .iter()
        .map(|(bucket, key)| (*bucket, key.as_ref()))
        .collect::<Vec<_>>();
    let rows = match if read_options_handle == 0 {
        db.multi_get(borrowed.as_slice())
    } else {
        let Some(options) =
            structured_read_options_from_handle_or_throw(&mut env, read_options_handle)
        else {
            return 0;
        };
        db.multi_get_with_options(borrowed.as_slice(), options.read_options())
    } {
        Ok(value) => value,
        Err(error) => {
            throw_illegal_state(&mut env, error.to_string());
            return 0;
        }
    };
    let required = match streaming_values_encoded_length(&rows) {
        Ok(value) => value,
        Err(error) => {
            throw_illegal_state(&mut env, error);
            return 0;
        }
    };
    let capacity = match usize::try_from(result_capacity) {
        Ok(value) => value,
        Err(_) => {
            throw_illegal_argument(&mut env, "resultCapacity must be >= 0".to_string());
            return 0;
        }
    };
    let required = match jint::try_from(required) {
        Ok(value) => value,
        Err(_) => {
            throw_illegal_state(
                &mut env,
                "streaming multi-get result is too large".to_string(),
            );
            return 0;
        }
    };
    if required as usize > capacity {
        return -required;
    }
    let address = match usize::try_from(result_address) {
        Ok(value) if value != 0 => value as *mut u8,
        _ => {
            throw_illegal_argument(&mut env, "resultAddress must be > 0".to_string());
            return 0;
        }
    };
    let output = unsafe { std::slice::from_raw_parts_mut(address, required as usize) };
    write_streaming_values(&rows, output);
    required
}

fn decode_batch_bounds(
    env: &mut JNIEnv,
    encoded_address: jlong,
    encoded_capacity: jint,
    encoded_length: jint,
    entry_count: jint,
) -> Option<(*const u8, usize, usize)> {
    let address = match usize::try_from(encoded_address) {
        Ok(value) if value != 0 => value as *const u8,
        _ => {
            throw_illegal_argument(env, "encodedAddress must be > 0".to_string());
            return None;
        }
    };
    let capacity = match usize::try_from(encoded_capacity) {
        Ok(value) => value,
        Err(_) => {
            throw_illegal_argument(env, "encodedCapacity must be >= 0".to_string());
            return None;
        }
    };
    let length = match usize::try_from(encoded_length) {
        Ok(value) if value <= capacity => value,
        _ => {
            throw_illegal_argument(
                env,
                format!(
                    "encodedLength {encoded_length} is outside encodedCapacity {encoded_capacity}"
                ),
            );
            return None;
        }
    };
    let count = match usize::try_from(entry_count) {
        Ok(value) => value,
        Err(_) => {
            throw_illegal_argument(env, "entryCount must be >= 0".to_string());
            return None;
        }
    };
    Some((address, length, count))
}

fn write_batch(
    db: &cobble_binding::structured::DataStructureDb,
    bucket: u16,
    column: u16,
    encoded: &[u8],
    entry_count: usize,
    options: &StructuredWriteOptions,
) -> Result<(), String> {
    validate_batch(encoded, entry_count)?;
    structured_ffi::db_put_bytes_batch_with_options(
        db,
        bucket,
        column,
        DirectBatchEntries::new(encoded, entry_count),
        options,
    )
    .map_err(|error| error.to_string())
}

fn validate_batch(encoded: &[u8], entry_count: usize) -> Result<(), String> {
    let mut offset = 0;
    for entry_index in 0..entry_count {
        let key_length = read_length(encoded, &mut offset, entry_index, "key")?;
        let value_length = read_length(encoded, &mut offset, entry_index, "value")?;
        let entry_length = key_length
            .checked_add(value_length)
            .ok_or_else(|| format!("direct write batch entry {entry_index} length overflow"))?;
        if entry_length > encoded.len().saturating_sub(offset) {
            return Err(format!(
                "direct write batch entry {entry_index} exceeds encoded payload"
            ));
        }
        offset += entry_length;
    }
    if offset != encoded.len() {
        return Err(format!(
            "direct write batch has {} trailing bytes",
            encoded.len() - offset
        ));
    }
    Ok(())
}

struct DirectBatchEntries<'a> {
    encoded: &'a [u8],
    offset: usize,
    remaining: usize,
}

impl<'a> DirectBatchEntries<'a> {
    fn new(encoded: &'a [u8], entry_count: usize) -> Self {
        Self {
            encoded,
            offset: 0,
            remaining: entry_count,
        }
    }
}

impl<'a> Iterator for DirectBatchEntries<'a> {
    type Item = (&'a [u8], &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        let key_length = read_validated_length(self.encoded, &mut self.offset);
        let value_length = read_validated_length(self.encoded, &mut self.offset);
        let key = &self.encoded[self.offset..self.offset + key_length];
        self.offset += key_length;
        let value = &self.encoded[self.offset..self.offset + value_length];
        self.offset += value_length;
        self.remaining -= 1;
        Some((key, value))
    }
}

fn read_validated_length(encoded: &[u8], offset: &mut usize) -> usize {
    let end = *offset + std::mem::size_of::<u32>();
    let bytes: [u8; 4] = encoded[*offset..end]
        .try_into()
        .expect("validated direct batch length");
    *offset = end;
    u32::from_be_bytes(bytes) as usize
}

fn read_length(
    encoded: &[u8],
    offset: &mut usize,
    entry_index: usize,
    role: &str,
) -> Result<usize, String> {
    let end = offset
        .checked_add(std::mem::size_of::<u32>())
        .ok_or_else(|| "direct write batch offset overflow".to_string())?;
    let bytes: [u8; 4] = encoded
        .get(*offset..end)
        .ok_or_else(|| format!("direct write batch entry {entry_index} is missing {role} length"))?
        .try_into()
        .expect("four-byte slice");
    *offset = end;
    Ok(u32::from_be_bytes(bytes) as usize)
}

fn decode_direct_chunks<'a>(
    env: &mut JNIEnv,
    addresses: &JLongArray,
    lengths: &JIntArray,
) -> Result<Vec<&'a [u8]>, String> {
    let address_count = env
        .get_array_length(addresses)
        .map_err(|error| error.to_string())? as usize;
    let length_count = env
        .get_array_length(lengths)
        .map_err(|error| error.to_string())? as usize;
    if address_count != length_count {
        return Err("chunkAddresses and chunkLengths differ in size".to_string());
    }
    let mut raw_addresses = vec![0i64; address_count];
    let mut raw_lengths = vec![0i32; length_count];
    env.get_long_array_region(addresses, 0, &mut raw_addresses)
        .map_err(|error| error.to_string())?;
    env.get_int_array_region(lengths, 0, &mut raw_lengths)
        .map_err(|error| error.to_string())?;

    raw_addresses
        .into_iter()
        .zip(raw_lengths)
        .enumerate()
        .map(|(index, (address, length))| {
            let length = usize::try_from(length)
                .map_err(|_| format!("chunkLengths[{index}] must be >= 0"))?;
            let address = usize::try_from(address)
                .ok()
                .filter(|value| *value != 0)
                .ok_or_else(|| format!("chunkAddresses[{index}] must be > 0"))?;
            // The Java caller retains every direct ByteBuffer for the duration of this JNI call.
            // These slices never escape the call or outlive their backing buffers.
            Ok(unsafe { std::slice::from_raw_parts(address as *const u8, length) })
        })
        .collect()
}

fn decode_non_negative_lengths(
    env: &mut JNIEnv,
    values: &JIntArray,
    name: &str,
    count: usize,
) -> Result<Vec<usize>, String> {
    let available = env
        .get_array_length(values)
        .map_err(|error| error.to_string())? as usize;
    if count > available {
        return Err(format!(
            "{name} contains {available} values but {count} are required"
        ));
    }
    let mut raw = vec![0i32; count];
    env.get_int_array_region(values, 0, &mut raw)
        .map_err(|error| error.to_string())?;
    raw.into_iter()
        .enumerate()
        .map(|(index, value)| {
            usize::try_from(value).map_err(|_| format!("{name}[{index}] must be >= 0"))
        })
        .collect()
}

fn decode_buckets(env: &mut JNIEnv, values: &JIntArray, count: usize) -> Result<Vec<u16>, String> {
    let available = env
        .get_array_length(values)
        .map_err(|error| error.to_string())? as usize;
    if count > available {
        return Err(format!(
            "buckets contains {available} values but {count} are required"
        ));
    }
    let mut raw = vec![0i32; count];
    env.get_int_array_region(values, 0, &mut raw)
        .map_err(|error| error.to_string())?;
    raw.into_iter()
        .enumerate()
        .map(|(index, value)| decode_u16(&format!("buckets[{index}]"), value))
        .collect()
}

fn decode_count(name: &str, value: jint) -> Result<usize, String> {
    usize::try_from(value).map_err(|_| format!("{name} must be >= 0"))
}

fn streaming_values_encoded_length(
    rows: &[Option<Vec<Option<StructuredColumnValue>>>],
) -> Result<usize, String> {
    i32::try_from(rows.len()).map_err(|_| "too many streaming multi-get rows".to_string())?;
    let mut length = std::mem::size_of::<i32>();
    for (index, row) in rows.iter().enumerate() {
        length = length
            .checked_add(std::mem::size_of::<i32>())
            .ok_or_else(|| "streaming multi-get result length overflow".to_string())?;
        if let Some(value) = streaming_bytes_value(row, index)? {
            i32::try_from(value.len())
                .map_err(|_| format!("streaming value {index} is too large"))?;
            length = length
                .checked_add(value.len())
                .ok_or_else(|| "streaming multi-get result length overflow".to_string())?;
        }
    }
    Ok(length)
}

fn write_streaming_values(rows: &[Option<Vec<Option<StructuredColumnValue>>>], output: &mut [u8]) {
    let mut offset = 0;
    write_streaming_i32(output, &mut offset, rows.len() as i32);
    for (index, row) in rows.iter().enumerate() {
        let value = streaming_bytes_value(row, index).expect("validated streaming value");
        match value {
            Some(value) => {
                write_streaming_i32(output, &mut offset, value.len() as i32);
                output[offset..offset + value.len()].copy_from_slice(value);
                offset += value.len();
            }
            None => write_streaming_i32(output, &mut offset, -1),
        }
    }
    debug_assert_eq!(offset, output.len());
}

fn streaming_bytes_value(
    row: &Option<Vec<Option<StructuredColumnValue>>>,
    index: usize,
) -> Result<Option<&[u8]>, String> {
    let Some(columns) = row else {
        return Ok(None);
    };
    if columns.len() != 1 {
        return Err(format!(
            "streaming multi-get row {index} has {} projected columns; expected one",
            columns.len()
        ));
    }
    match &columns[0] {
        None => Ok(None),
        Some(StructuredColumnValue::Bytes(value)) => Ok(Some(value)),
        Some(StructuredColumnValue::List(_)) => Err(format!(
            "streaming multi-get row {index} is a list; expected a bytes column"
        )),
    }
}

fn write_streaming_i32(output: &mut [u8], offset: &mut usize, value: i32) {
    let end = *offset + std::mem::size_of::<i32>();
    output[*offset..end].copy_from_slice(&value.to_be_bytes());
    *offset = end;
}

struct DirectChunksCursor<'a> {
    chunks: Vec<&'a [u8]>,
    chunk_index: usize,
    offset: usize,
}

impl<'a> DirectChunksCursor<'a> {
    fn new(chunks: Vec<&'a [u8]>) -> Self {
        Self {
            chunks,
            chunk_index: 0,
            offset: 0,
        }
    }

    fn take(&mut self, length: usize) -> Result<Cow<'a, [u8]>, String> {
        if length == 0 {
            return Ok(Cow::Borrowed(&[]));
        }
        self.advance_empty_chunks();
        if let Some(chunk) = self.chunks.get(self.chunk_index) {
            let remaining = chunk.len().saturating_sub(self.offset);
            if length <= remaining {
                let start = self.offset;
                self.offset += length;
                return Ok(Cow::Borrowed(&chunk[start..start + length]));
            }
        }

        let mut value = Vec::with_capacity(length);
        let mut remaining = length;
        while remaining > 0 {
            self.advance_empty_chunks();
            let Some(chunk) = self.chunks.get(self.chunk_index) else {
                return Err(format!(
                    "streaming direct chunks ended with {remaining} bytes still required"
                ));
            };
            let copied = remaining.min(chunk.len() - self.offset);
            value.extend_from_slice(&chunk[self.offset..self.offset + copied]);
            self.offset += copied;
            remaining -= copied;
        }
        Ok(Cow::Owned(value))
    }

    fn ensure_exhausted(&mut self) -> Result<(), String> {
        self.advance_empty_chunks();
        if self.chunk_index == self.chunks.len() {
            Ok(())
        } else {
            let remaining = self.chunks[self.chunk_index..]
                .iter()
                .map(|chunk| chunk.len())
                .sum::<usize>()
                .saturating_sub(self.offset);
            Err(format!(
                "streaming direct chunks have {remaining} trailing bytes"
            ))
        }
    }

    fn advance_empty_chunks(&mut self) {
        while let Some(chunk) = self.chunks.get(self.chunk_index) {
            if self.offset < chunk.len() {
                return;
            }
            self.chunk_index += 1;
            self.offset = 0;
        }
    }
}
