use crate::structured::db_from_handle;
use crate::structured_write_options::structured_write_options_from_handle_or_throw;
use crate::util::{decode_u16, throw_illegal_argument, throw_illegal_state};
use cobble_data_structure::StructuredWriteOptions;
use jni::JNIEnv;
use jni::objects::JClass;
use jni::sys::{jint, jlong};

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
    db: &cobble_data_structure::DataStructureDb,
    bucket: u16,
    column: u16,
    encoded: &[u8],
    entry_count: usize,
    options: &StructuredWriteOptions,
) -> Result<(), String> {
    validate_batch(encoded, entry_count)?;
    db.put_bytes_batch_with_options(
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
