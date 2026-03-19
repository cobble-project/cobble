use crate::error::{Error, Result};
use crate::file::{File, SequentialWriteFile};
use crate::format::FileBuilder;
use crate::parquet::file_adapter::SequentialWriteFileAdapter;
use crate::parquet::meta::{ParquetMeta, ParquetRowGroupRange};
use bytes::Bytes;
use parquet::data_type::{ByteArray, ByteArrayType};
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use std::mem;
use std::sync::Arc;

/// TODO: use column-aware schema.
const PARQUET_KV_SCHEMA: &str = r#"
message schema {
  REQUIRED BINARY key;
  REQUIRED BINARY value;
}
"#;

const DEFAULT_PARQUET_ROW_GROUP_SIZE_BYTES: usize = 256 * 1024;

#[derive(Clone, Debug)]
pub(crate) struct ParquetWriterOptions {
    pub(crate) row_group_size_bytes: usize,
    pub(crate) buffer_size: usize,
}

impl Default for ParquetWriterOptions {
    fn default() -> Self {
        Self {
            row_group_size_bytes: DEFAULT_PARQUET_ROW_GROUP_SIZE_BYTES,
            buffer_size: 8192,
        }
    }
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
    values: Vec<ByteArray>,
    row_group_ranges: Vec<ParquetRowGroupRange>,
}

impl<W: SequentialWriteFile + Send> ParquetWriter<W> {
    pub(crate) fn new(writer: W) -> Result<Self> {
        Self::with_options(writer, ParquetWriterOptions::default())
    }

    pub(crate) fn with_options(writer: W, options: ParquetWriterOptions) -> Result<Self> {
        let row_group_size_bytes = options.row_group_size_bytes.max(1);
        let schema =
            Arc::new(parse_message_type(PARQUET_KV_SCHEMA).map_err(|err| {
                Error::IoError(format!("Failed to parse parquet schema: {}", err))
            })?);
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
                ..options
            },
            total_rows: 0,
            first_key: None,
            last_key: Vec::new(),
            current_group_start_key: None,
            current_group_end_key: Vec::new(),
            current_group_bytes: 0,
            keys: Vec::new(),
            values: Vec::new(),
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
                .map_err(|err| Error::IoError(format!("Failed to open value column: {}", err)))?
                .ok_or_else(|| Error::IoError("Missing value column".to_string()))?;
            col.typed::<ByteArrayType>()
                .write_batch(&self.values, None, None)
                .map_err(|err| Error::IoError(format!("Failed to write value column: {}", err)))?;
            col.close()
                .map_err(|err| Error::IoError(format!("Failed to close value column: {}", err)))?;
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
        self.values.clear();
        self.current_group_bytes = 0;
        Ok(())
    }

    pub(crate) fn add(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        if !self.last_key.is_empty() && key <= self.last_key.as_slice() {
            return Err(Error::IoError(format!(
                "Keys must be added in sorted order: {:?} <= {:?}",
                key, self.last_key
            )));
        }
        if self.first_key.is_none() {
            self.first_key = Some(key.to_vec());
        }

        let entry_bytes = key.len() + value.len();
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
        self.values
            .push(ByteArray::from(Bytes::copy_from_slice(value)));
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
        self.writer.bytes_written() as usize
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.first_key.is_none()
    }
}

impl<W: SequentialWriteFile + Send + 'static> FileBuilder for ParquetWriter<W> {
    fn add(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        ParquetWriter::add(self, key, value)
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
