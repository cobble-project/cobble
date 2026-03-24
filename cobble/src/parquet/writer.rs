use crate::error::{Error, Result};
use crate::file::{File, SequentialWriteFile};
use crate::format::FileBuilder;
use crate::parquet::file_adapter::SequentialWriteFileAdapter;
use crate::parquet::meta::{ParquetMeta, ParquetRowGroupRange};
use crate::sst::row_codec::decode_value;
use crate::r#type::{KvValue, Value};
use bytes::Bytes;
use parquet::data_type::{ByteArray, ByteArrayType, Int64Type};
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use std::mem;
use std::sync::Arc;

const DEFAULT_PARQUET_ROW_GROUP_SIZE_BYTES: usize = 256 * 1024;

#[derive(Clone, Debug)]
pub(crate) struct ParquetWriterOptions {
    pub(crate) row_group_size_bytes: usize,
    pub(crate) buffer_size: usize,
    pub(crate) num_columns: usize,
}

impl Default for ParquetWriterOptions {
    fn default() -> Self {
        Self {
            row_group_size_bytes: DEFAULT_PARQUET_ROW_GROUP_SIZE_BYTES,
            buffer_size: 8192,
            num_columns: 1,
        }
    }
}

fn parquet_schema(num_columns: usize) -> String {
    let mut schema =
        String::from("message schema {\n  REQUIRED BINARY key;\n  REQUIRED INT64 expired_at;\n");
    for idx in 0..num_columns {
        schema.push_str(&format!("  OPTIONAL BINARY c{};\n", idx));
    }
    schema.push('}');
    schema
}

pub(crate) struct ParquetWriter<W: SequentialWriteFile> {
    writer: SerializedFileWriter<SequentialWriteFileAdapter<W>>,
    options: ParquetWriterOptions,
    total_rows: u64,
    first_key: Option<Vec<u8>>,
    last_key: Vec<u8>,
    current_group_start_key: Option<Vec<u8>>,
    current_group_end_key: Vec<u8>,
    current_group_bytes: usize,
    keys: Vec<ByteArray>,
    expired_ats: Vec<i64>,
    column_payload_values: Vec<Vec<ByteArray>>,
    column_payload_def_levels: Vec<Vec<i16>>,
    row_group_ranges: Vec<ParquetRowGroupRange>,
}

impl<W: SequentialWriteFile + Send> ParquetWriter<W> {
    pub(crate) fn new(writer: W) -> Result<Self> {
        Self::with_options(writer, ParquetWriterOptions::default())
    }

    pub(crate) fn with_options(writer: W, options: ParquetWriterOptions) -> Result<Self> {
        let num_columns = options.num_columns.max(1);
        let row_group_size_bytes = options.row_group_size_bytes.max(1);
        let schema = Arc::new(
            parse_message_type(&parquet_schema(num_columns)).map_err(|err| {
                Error::IoError(format!("Failed to parse parquet schema: {}", err))
            })?,
        );
        let writer = SerializedFileWriter::new(
            SequentialWriteFileAdapter::new_buffered(writer, options.buffer_size),
            schema,
            Default::default(),
        )
        .map_err(|err| Error::IoError(format!("Failed to create parquet writer: {}", err)))?;
        Ok(Self {
            writer,
            options: ParquetWriterOptions {
                row_group_size_bytes,
                num_columns,
                ..options
            },
            total_rows: 0,
            first_key: None,
            last_key: Vec::new(),
            current_group_start_key: None,
            current_group_end_key: Vec::new(),
            current_group_bytes: 0,
            keys: Vec::new(),
            expired_ats: Vec::new(),
            column_payload_values: vec![Vec::new(); num_columns],
            column_payload_def_levels: vec![Vec::new(); num_columns],
            row_group_ranges: Vec::new(),
        })
    }

    fn flush_current_group(&mut self) -> Result<()> {
        if self.keys.is_empty() {
            return Ok(());
        }
        let mut row_group = self
            .writer
            .next_row_group()
            .map_err(|err| Error::IoError(format!("Failed to create row group: {}", err)))?;
        {
            let mut col = row_group
                .next_column()
                .map_err(|err| Error::IoError(format!("Failed to open key column: {}", err)))?
                .ok_or_else(|| Error::IoError("Missing key column".to_string()))?;
            col.typed::<ByteArrayType>()
                .write_batch(&self.keys, None, None)
                .map_err(|err| Error::IoError(format!("Failed to write key column: {}", err)))?;
            col.close()
                .map_err(|err| Error::IoError(format!("Failed to close key column: {}", err)))?;
        }
        {
            let mut col = row_group
                .next_column()
                .map_err(|err| {
                    Error::IoError(format!("Failed to open expired_at column: {}", err))
                })?
                .ok_or_else(|| Error::IoError("Missing expired_at column".to_string()))?;
            col.typed::<Int64Type>()
                .write_batch(&self.expired_ats, None, None)
                .map_err(|err| {
                    Error::IoError(format!("Failed to write expired_at column: {}", err))
                })?;
            col.close().map_err(|err| {
                Error::IoError(format!("Failed to close expired_at column: {}", err))
            })?;
        }
        for idx in 0..self.options.num_columns {
            let mut col = row_group
                .next_column()
                .map_err(|err| {
                    Error::IoError(format!("Failed to open column payload {}: {}", idx, err))
                })?
                .ok_or_else(|| Error::IoError(format!("Missing parquet payload column {}", idx)))?;
            col.typed::<ByteArrayType>()
                .write_batch(
                    &self.column_payload_values[idx],
                    Some(&self.column_payload_def_levels[idx]),
                    None,
                )
                .map_err(|err| {
                    Error::IoError(format!(
                        "Failed to write parquet payload column {}: {}",
                        idx, err
                    ))
                })?;
            col.close().map_err(|err| {
                Error::IoError(format!(
                    "Failed to close parquet payload column {}: {}",
                    idx, err
                ))
            })?;
        }
        row_group
            .close()
            .map_err(|err| Error::IoError(format!("Failed to close row group: {}", err)))?;

        let start_key = self.current_group_start_key.take().ok_or_else(|| {
            Error::IoError("Parquet row group missing start key during flush".to_string())
        })?;
        let end_key = mem::take(&mut self.current_group_end_key);
        self.row_group_ranges
            .push(ParquetRowGroupRange { start_key, end_key });
        self.keys.clear();
        self.expired_ats.clear();
        for col_values in &mut self.column_payload_values {
            col_values.clear();
        }
        for col_levels in &mut self.column_payload_def_levels {
            col_levels.clear();
        }
        self.current_group_bytes = 0;
        Ok(())
    }

    pub(crate) fn add(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut encoded_value = Bytes::copy_from_slice(value);
        let decoded = decode_value(&mut encoded_value, self.options.num_columns)?;
        self.add_value(
            key,
            &decoded,
            key.len() + value.len(),
            self.options.num_columns,
        )
    }

    fn add_value(
        &mut self,
        key: &[u8],
        decoded: &Value,
        entry_bytes: usize,
        num_columns: usize,
    ) -> Result<()> {
        if !self.last_key.is_empty() && key <= self.last_key.as_slice() {
            return Err(Error::IoError(format!(
                "Keys must be added in sorted order: {:?} <= {:?}",
                key, self.last_key
            )));
        }
        if self.first_key.is_none() {
            self.first_key = Some(key.to_vec());
        }

        let should_flush = !self.keys.is_empty()
            && self.current_group_bytes + entry_bytes > self.options.row_group_size_bytes;
        if should_flush {
            self.flush_current_group()?;
        }

        if self.current_group_start_key.is_none() {
            self.current_group_start_key = Some(key.to_vec());
        }
        self.current_group_end_key = key.to_vec();
        self.current_group_bytes += entry_bytes;
        self.last_key = key.to_vec();
        self.total_rows += 1;
        self.keys.push(ByteArray::from(Bytes::copy_from_slice(key)));
        self.expired_ats
            .push(decoded.expired_at().unwrap_or(0) as i64);
        for idx in 0..num_columns {
            if let Some(column) = decoded.columns().get(idx).and_then(|col| col.as_ref()) {
                let mut payload = Vec::with_capacity(1 + column.data().len());
                payload.push(column.value_type().encode_tag());
                payload.extend_from_slice(column.data());
                self.column_payload_values[idx].push(ByteArray::from(Bytes::from(payload)));
                self.column_payload_def_levels[idx].push(1);
            } else {
                self.column_payload_def_levels[idx].push(0);
            }
        }
        Ok(())
    }

    pub(crate) fn finish(mut self) -> Result<(Vec<u8>, Vec<u8>, usize, Bytes)> {
        let first_key = self.first_key.clone().unwrap_or_default();
        self.flush_current_group()?;
        let last_key = self.last_key;
        let mut adapter = self
            .writer
            .into_inner()
            .map_err(|err| Error::IoError(format!("Failed to finalize parquet file: {}", err)))?;
        let file_size = adapter.size();
        adapter.close()?;
        Ok((
            first_key,
            last_key,
            file_size,
            ParquetMeta::new(self.total_rows, self.row_group_ranges).encode(),
        ))
    }

    pub(crate) fn offset(&self) -> usize {
        self.writer.bytes_written()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.first_key.is_none()
    }
}

impl<W: SequentialWriteFile + Send + 'static> FileBuilder for ParquetWriter<W> {
    fn add(&mut self, key: &[u8], value: &KvValue) -> Result<()> {
        match value {
            KvValue::Encoded(bytes) => ParquetWriter::add(self, key, bytes),
            KvValue::Decoded(v) => {
                // Estimate entry size for row group flushing decisions.
                let value_bytes_estimate = v
                    .columns()
                    .iter()
                    .map(|c| c.as_ref().map_or(0, |col| 1 + col.data().len()))
                    .sum::<usize>()
                    + 4;
                // Use the decoded value's column count for consistent encoding.
                let num_cols = v.columns().len().min(self.options.num_columns);
                self.add_value(key, v, key.len() + value_bytes_estimate, num_cols)
            }
        }
    }

    fn finish(self: Box<Self>) -> Result<(Vec<u8>, Vec<u8>, usize, Bytes)> {
        (*self).finish()
    }

    fn offset(&self) -> usize {
        ParquetWriter::offset(self)
    }

    fn is_empty(&self) -> bool {
        ParquetWriter::is_empty(self)
    }
}
