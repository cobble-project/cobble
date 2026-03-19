use crate::data_file::DataFile;
use crate::error::{Error, Result};
use crate::file::RandomAccessFile;
use crate::iterator::KvIterator;
use crate::parquet::file_adapter::RandomAccessChunkReader;
use crate::parquet::meta::{ParquetRowGroupRange, decode_meta_row_group_ranges};
use bytes::Bytes;
use parquet::column::reader::{ColumnReaderImpl, get_typed_column_reader};
use parquet::data_type::{ByteArray, ByteArrayType};
use parquet::file::reader::FileReader;
use parquet::file::serialized_reader::SerializedFileReader;
use std::sync::Arc;

const PARQUET_ITERATOR_BATCH_ROWS: usize = 256;

type ByteArrayColumnReader = ColumnReaderImpl<ByteArrayType>;

pub(crate) struct ParquetIterator {
    file: Arc<dyn RandomAccessFile>,
    row_group_ranges: Option<Vec<ParquetRowGroupRange>>,
    total_row_groups: usize,
    current_row_group_idx: usize,
    current_group_rows: usize,
    current_group_rows_loaded: usize,
    key_reader: Option<ByteArrayColumnReader>,
    value_reader: Option<ByteArrayColumnReader>,
    batch_keys: Vec<ByteArray>,
    batch_values: Vec<ByteArray>,
    current_idx: Option<usize>,
    previous_key: Option<Bytes>,
}

impl ParquetIterator {
    pub(crate) fn new(file: Box<dyn RandomAccessFile>) -> Result<Self> {
        Self::new_with_ranges(file, None)
    }

    pub(crate) fn from_data_file(
        file: Box<dyn RandomAccessFile>,
        data_file: &DataFile,
    ) -> Result<Self> {
        let row_group_ranges = decode_meta_row_group_ranges(data_file.meta_bytes())?;
        Self::new_with_ranges(file, row_group_ranges)
    }

    fn new_with_ranges(
        file: Box<dyn RandomAccessFile>,
        row_group_ranges: Option<Vec<ParquetRowGroupRange>>,
    ) -> Result<Self> {
        let file = Arc::from(file);
        let total_row_groups = Self::row_group_count(&file)?;
        let row_group_ranges = row_group_ranges.filter(|ranges| ranges.len() == total_row_groups);
        Ok(Self {
            file,
            row_group_ranges,
            total_row_groups,
            current_row_group_idx: 0,
            current_group_rows: 0,
            current_group_rows_loaded: 0,
            key_reader: None,
            value_reader: None,
            batch_keys: Vec::new(),
            batch_values: Vec::new(),
            current_idx: None,
            previous_key: None,
        })
    }

    fn row_group_count(file: &Arc<dyn RandomAccessFile>) -> Result<usize> {
        let chunk_reader = RandomAccessChunkReader::from_arc(Arc::clone(file));
        let reader = SerializedFileReader::new(chunk_reader)
            .map_err(|err| Error::IoError(format!("Failed to open parquet reader: {}", err)))?;
        let metadata = reader.metadata();
        let count = metadata.num_row_groups();
        if count == 0 {
            return Ok(0);
        }
        for idx in 0..count {
            let row_group = reader.get_row_group(idx).map_err(|err| {
                Error::IoError(format!("Failed to get parquet row group {}: {}", idx, err))
            })?;
            if row_group.num_columns() != 2 {
                return Err(Error::IoError(format!(
                    "Unsupported parquet kv column count: {}",
                    row_group.num_columns()
                )));
            }
        }
        Ok(count)
    }

    fn open_row_group_readers(
        file: &Arc<dyn RandomAccessFile>,
        row_group_idx: usize,
    ) -> Result<(usize, ByteArrayColumnReader, ByteArrayColumnReader)> {
        let chunk_reader = RandomAccessChunkReader::from_arc(Arc::clone(file));
        let reader = SerializedFileReader::new(chunk_reader)
            .map_err(|err| Error::IoError(format!("Failed to open parquet reader: {}", err)))?;
        let row_group = reader.get_row_group(row_group_idx).map_err(|err| {
            Error::IoError(format!(
                "Failed to get parquet row group {}: {}",
                row_group_idx, err
            ))
        })?;
        if row_group.num_columns() != 2 {
            return Err(Error::IoError(format!(
                "Unsupported parquet kv column count: {}",
                row_group.num_columns()
            )));
        }
        let row_count = row_group.metadata().num_rows() as usize;
        let key_reader = row_group
            .get_column_reader(0)
            .map_err(|err| Error::IoError(format!("Failed to get key column reader: {}", err)))
            .map(get_typed_column_reader::<ByteArrayType>)?;
        let value_reader = row_group
            .get_column_reader(1)
            .map_err(|err| Error::IoError(format!("Failed to get value column reader: {}", err)))
            .map(get_typed_column_reader::<ByteArrayType>)?;
        Ok((row_count, key_reader, value_reader))
    }

    fn reset_readers_from_group(&mut self, start_row_group_idx: usize) {
        self.current_row_group_idx = start_row_group_idx.min(self.total_row_groups);
        self.current_group_rows = 0;
        self.current_group_rows_loaded = 0;
        self.key_reader = None;
        self.value_reader = None;
        self.batch_keys.clear();
        self.batch_values.clear();
        self.current_idx = None;
        self.previous_key = None;
    }

    fn ensure_active_row_group(&mut self) -> Result<bool> {
        while self.current_row_group_idx < self.total_row_groups {
            if self.key_reader.is_some()
                && self.value_reader.is_some()
                && self.current_group_rows_loaded < self.current_group_rows
            {
                return Ok(true);
            }
            let (rows, key_reader, value_reader) =
                Self::open_row_group_readers(&self.file, self.current_row_group_idx)?;
            self.current_group_rows = rows;
            self.current_group_rows_loaded = 0;
            self.key_reader = Some(key_reader);
            self.value_reader = Some(value_reader);
            if rows > 0 {
                return Ok(true);
            }
            self.current_row_group_idx += 1;
            self.key_reader = None;
            self.value_reader = None;
        }
        Ok(false)
    }

    fn load_next_batch(&mut self) -> Result<bool> {
        loop {
            if !self.ensure_active_row_group()? {
                self.batch_keys.clear();
                self.batch_values.clear();
                self.current_idx = None;
                return Ok(false);
            }
            let to_read = (self.current_group_rows - self.current_group_rows_loaded)
                .min(PARQUET_ITERATOR_BATCH_ROWS);
            self.batch_keys.clear();
            self.batch_values.clear();

            let key_reader = self
                .key_reader
                .as_mut()
                .ok_or_else(|| Error::IoError("Missing parquet key reader".to_string()))?;
            let value_reader = self
                .value_reader
                .as_mut()
                .ok_or_else(|| Error::IoError("Missing parquet value reader".to_string()))?;

            let (key_records, key_values, _) = key_reader
                .read_records(to_read, None, None, &mut self.batch_keys)
                .map_err(|err| {
                    Error::IoError(format!("Failed to read key column records: {}", err))
                })?;
            let (value_records, value_values, _) = value_reader
                .read_records(to_read, None, None, &mut self.batch_values)
                .map_err(|err| {
                    Error::IoError(format!("Failed to read value column records: {}", err))
                })?;

            if key_records == 0 && value_records == 0 {
                self.current_row_group_idx += 1;
                self.key_reader = None;
                self.value_reader = None;
                continue;
            }
            if key_records != value_records || key_values != value_values {
                return Err(Error::IoError(format!(
                    "Parquet key/value batch mismatch: key_records={} value_records={} key_values={} value_values={}",
                    key_records, value_records, key_values, value_values
                )));
            }
            if self.batch_keys.len() != self.batch_values.len() {
                return Err(Error::IoError(format!(
                    "Parquet key/value row count mismatch: {} vs {}",
                    self.batch_keys.len(),
                    self.batch_values.len()
                )));
            }
            if let (Some(prev), Some(first)) = (self.previous_key.as_ref(), self.batch_keys.first())
                && prev.as_ref() >= first.data()
            {
                return Err(Error::IoError(
                    "Parquet rows are not strictly sorted by key".to_string(),
                ));
            }
            for idx in 1..self.batch_keys.len() {
                if self.batch_keys[idx - 1].data() >= self.batch_keys[idx].data() {
                    return Err(Error::IoError(
                        "Parquet rows are not strictly sorted by key".to_string(),
                    ));
                }
            }

            self.current_group_rows_loaded += key_records;
            if self.current_group_rows_loaded >= self.current_group_rows {
                self.current_row_group_idx += 1;
            }
            self.current_idx = (!self.batch_keys.is_empty()).then_some(0);
            return Ok(self.current_idx.is_some());
        }
    }

    pub(crate) fn seek(&mut self, target: &[u8]) -> Result<()> {
        let start_group = self
            .row_group_ranges
            .as_ref()
            .map(|ranges| ranges.partition_point(|range| range.end_key.as_slice() < target))
            .unwrap_or(0);
        self.reset_readers_from_group(start_group);
        loop {
            if !self.load_next_batch()? {
                return Ok(());
            }
            let Some(last_key) = self.batch_keys.last() else {
                continue;
            };
            if last_key.data() < target {
                self.previous_key = Some(Bytes::copy_from_slice(last_key.data()));
                continue;
            }
            let idx = self.batch_keys.partition_point(|key| key.data() < target);
            self.current_idx = (idx < self.batch_keys.len()).then_some(idx);
            return Ok(());
        }
    }

    pub(crate) fn seek_to_first(&mut self) -> Result<()> {
        self.reset_readers_from_group(0);
        let _ = self.load_next_batch()?;
        Ok(())
    }

    pub(crate) fn next(&mut self) -> Result<bool> {
        let Some(current) = self.current_idx else {
            return Ok(false);
        };
        let next = current + 1;
        if next < self.batch_keys.len() {
            self.current_idx = Some(next);
            return Ok(true);
        }
        if let Some(last_key) = self.batch_keys.last() {
            self.previous_key = Some(Bytes::copy_from_slice(last_key.data()));
        }
        self.load_next_batch()
    }

    pub(crate) fn valid(&self) -> bool {
        self.current_idx
            .map(|idx| idx < self.batch_keys.len())
            .unwrap_or(false)
    }

    pub(crate) fn key(&self) -> Result<Option<Bytes>> {
        Ok(self
            .current_idx
            .map(|idx| Bytes::copy_from_slice(self.batch_keys[idx].data())))
    }

    pub(crate) fn value(&self) -> Result<Option<Bytes>> {
        Ok(self
            .current_idx
            .map(|idx| Bytes::copy_from_slice(self.batch_values[idx].data())))
    }
}

impl<'a> KvIterator<'a> for ParquetIterator {
    fn seek(&mut self, target: &[u8]) -> Result<()> {
        ParquetIterator::seek(self, target)
    }

    fn seek_to_first(&mut self) -> Result<()> {
        ParquetIterator::seek_to_first(self)
    }

    fn next(&mut self) -> Result<bool> {
        ParquetIterator::next(self)
    }

    fn valid(&self) -> bool {
        ParquetIterator::valid(self)
    }

    fn key(&self) -> Result<Option<Bytes>> {
        ParquetIterator::key(self)
    }

    fn key_slice(&self) -> Result<Option<&[u8]>> {
        if let Some(idx) = self.current_idx {
            return Ok(self.batch_keys.get(idx).map(|key| key.data()));
        }
        Ok(None)
    }

    fn value(&self) -> Result<Option<Bytes>> {
        ParquetIterator::value(self)
    }

    fn value_slice(&self) -> Result<Option<&[u8]>> {
        if let Some(idx) = self.current_idx {
            return Ok(self.batch_values.get(idx).map(|value| value.data()));
        }
        Ok(None)
    }
}
